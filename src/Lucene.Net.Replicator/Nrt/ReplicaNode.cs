/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using J2N;
using Lucene.Net.Index;
using Lucene.Net.Replicator.Nrt;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;
using Directory = Lucene.Net.Store.Directory;
using Version = Lucene.Net.Util.LuceneVersion;
using JCG = J2N.Collections.Generic;
using Lucene;
using Lucene.Net.Diagnostics;
using Lucene.Net.Support;
using System.Linq;
using Lucene.Net.Support.Threading;
using System.Collections.ObjectModel;
using J2N.Threading.Atomic;

namespace Lucene.Net.Replicator.Nrt
{
    ///<summary>
    /// Replica node, that pulls index changes from the primary node by copying newly flushed or merged
    /// index files.
    ///</summary>
    ///<remarks>
    /// @lucene.experimental
    ///</remarks>
    public abstract class ReplicaNode : Node
    {

        internal ReplicaFileDeleter deleter;


        ///<summary>
        /// IncRef'd files in the current commit point: 
        /// </summary>
        private readonly ICollection<string> lastCommitFiles = new JCG.HashSet<string>();

        ///<summary>
        /// IncRef'd files in the current NRT point:
        /// </summary>
        protected readonly ICollection<string> lastNRTFiles = new JCG.HashSet<string>();

        ///<summary>
        /// Currently running merge pre-copy jobs
        /// </summary>
        protected readonly ISet<CopyJob> mergeCopyJobs = new ConcurrentHashSet<CopyJob>();

        ///<summary>
        ///  Non-null when we are currently copying files from a new NRT point:
        /// </summary>
        protected CopyJob curNRTCopy;

        ///<summary>
        /// We hold this to ensure an external IndexWriter cannot also open on our directory:
        /// </summary>
        private readonly Lock writeFileLock;

        ///<summary>
        /// Merged segment files that we pre-copied, but have not yet made visible in a new NRT point.
        /// </summary>
        readonly ISet<string> pendingMergeFiles = new ConcurrentHashSet<string>();

        ///<summary>
        /// Primary gen last time we successfully replicated:
        /// </summary>
        protected long lastPrimaryGen;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="dir"></param>
        /// <param name="searcherFactory"></param>
        /// <param name="textWriter"></param>
        /// <exception cref="IOException"/>
        public ReplicaNode(int id, Directory dir, SearcherFactory searcherFactory, TextWriter textWriter) : base(id, dir, searcherFactory, textWriter)
        {

            if (dir.GetPendingDeletions().Any())
            {
                throw new IllegalArgumentException(
                    "Directory " + dir + " still has pending deleted files; cannot initialize IndexWriter");
            }

            bool success = false;

            try
            {
                Message("top: init replica dir=" + dir);

                // Obtain a write lock on this index since we "act like" an IndexWriter, to prevent any other
                // IndexWriter or ReplicaNode from using it:
                writeFileLock = dir.ObtainLock(IndexWriter.WRITE_LOCK_NAME);

                state = "init";
                deleter = new ReplicaFileDeleter(this, dir);
                success = true;
            }
            catch (Exception t)
            {
                Message("exc on init:");
                t.PrintStackTrace(textWriter);
                throw;
            }
            finally
            {
                if (success == false)
                {
                    IOUtils.CloseWhileHandlingException(this);
                }
            }
        }

        ///<summary>
        /// Start up this replica, which possibly requires heavy copying of files from the primary node, if
        /// we were down for a long time
        /// </summary>
        /// <exception cref="IOException"/>
        protected void Start(long curPrimaryGen)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                if (state.Equals("init") == false)
                {
                    throw IllegalStateException.Create("already started");
                }

                Message("top: now start");
                try
                {

                    // Figure out what state our local index is in now:
                    String segmentsFileName = SegmentInfos.GetLastCommitSegmentsFileName(dir);

                    // Also look for any pending_segments_N, in case we crashed mid-commit.  We must "inflate" our
                    // infos gen to at least this, since
                    // otherwise we may wind up re-using the pending_segments_N file name on commit, and then our
                    // deleter can get angry because it still
                    // wants to delete this file:
                    long maxPendingGen = -1;
                    foreach (string fileName in dir.ListAll())
                    {
                        if (fileName.StartsWith(IndexFileNames.PENDING_SEGMENTS))
                        {
                            long gen =
                                long.Parse(
                                    fileName.Substring(IndexFileNames.PENDING_SEGMENTS.Length + 1),
                                    Character.MaxRadix);
                            if (gen > maxPendingGen)
                            {
                                maxPendingGen = gen;
                            }
                        }
                    }

                    SegmentInfos infos;
                    if (segmentsFileName == null)
                    {
                        // No index here yet:
                        infos = new SegmentInfos(LuceneVersions.LATEST_MAJOR);
                        Message("top: init: no segments in index");
                    }
                    else
                    {
                        Message("top: init: read existing segments commit " + segmentsFileName);
                        infos = SegmentInfos.ReadCommit(dir, segmentsFileName);
                        Message("top: init: segments: " + infos.ToString() + " version=" + infos.Version);
                        ICollection<string> indexFiles = infos.GetFiles(dir, false);

                        lastCommitFiles.Add(segmentsFileName);
                        lastCommitFiles.AddRange(indexFiles);

                        // Always protect the last commit:
                        deleter.IncRef(lastCommitFiles);

                        lastNRTFiles.AddRange(indexFiles);
                        deleter.IncRef(lastNRTFiles);
                        Message("top: commitFiles=" + lastCommitFiles);
                        Message("top: nrtFiles=" + lastNRTFiles);
                    }

                    Message("top: delete unknown files on init: all files=" + Arrays.ToString(dir.ListAll()));
                    deleter.DeleteUnknownFiles(segmentsFileName);
                    Message(
                        "top: done delete unknown files on init: all files=" + Arrays.ToString(dir.ListAll()));

                    string s = infos.GetUserData().get(PRIMARY_GEN_KEY);
                    long myPrimaryGen;
                    if (s == null)
                    {
                        if (Debugging.AssertsEnabled)
                        {
                            Debugging.Assert(infos.Size() == 0);
                        }
                        myPrimaryGen = -1;
                    }
                    else
                    {
                        myPrimaryGen = long.Parse(s);
                    }
                    Message("top: myPrimaryGen=" + myPrimaryGen);

                    bool doCommit;

                    if (infos.Size() > 0 && myPrimaryGen != -1 && myPrimaryGen != curPrimaryGen)
                    {
                        if (Debugging.AssertsEnabled)
                        {
                            Debugging.Assert(myPrimaryGen < curPrimaryGen);
                        }

                        // Primary changed while we were down.  In this case, we must sync from primary before
                        // opening a reader, because it's possible current
                        // files we have will need to be overwritten with different ones (if index rolled back and
                        // "forked"), and we can't overwrite open
                        // files on Windows:

                        /*readonly*/
                        long initSyncStartNS = Time.NanoTime();

                        Message(
                            "top: init: primary changed while we were down myPrimaryGen="
                                + myPrimaryGen
                                + " vs curPrimaryGen="
                                + curPrimaryGen
                                + "; sync now before mgr init");

                        // Try until we succeed in copying over the latest NRT point:
                        CopyJob job = null;

                        // We may need to overwrite files referenced by our latest commit, either right now on
                        // initial sync, or on a later sync.  To make
                        // sure the index is never even in an "apparently" corrupt state (where an old segments_N
                        // references invalid files) we forcefully
                        // remove the commit now, and refuse to start the replica if this delete fails:
                        Message("top: now delete starting commit point " + segmentsFileName);

                        // If this throws exc (e.g. due to virus checker), we cannot start this replica:
                        if (Debugging.AssertsEnabled)
                        {
                            Debugging.Assert(deleter.GetRefCount(segmentsFileName) == 1);
                        }
                        deleter.DecRef(new List<string>(){
                        segmentsFileName
                    });

                        if (dir.GetPendingDeletions().Any())
                        {
                            // If e.g. virus checker blocks us from deleting, we absolutely cannot start this node
                            // else there is a definite window during
                            // which if we carsh, we cause corruption:
                            throw  RuntimeException.Create(
                                "replica cannot start: existing segments file="
                                    + segmentsFileName
                                    + " must be removed in order to start, but the file delete failed");
                        }

                        // So we don't later try to decRef it (illegally) again:
                        bool didRemove = lastCommitFiles.Remove(segmentsFileName);
                        if (Debugging.AssertsEnabled)
                        {
                            Debugging.Assert(didRemove);
                        }

                        while (true)
                        {
                            job =
                                NewCopyJob(
                                    "sync on startup replica=" + Name() + " myVersion=" + infos.Version,
                                    null,
                                    null,
                                    true,
                                    null);
                            job.Start();

                            Message("top: init: sync sis.version=" + job.GetCopyState().version);

                            // Force this copy job to finish while we wait, now.  Note that this can be very time
                            // consuming!
                            // NOTE: newNRTPoint detects we are still in init (mgr is null) and does not cancel our
                            // copy if a flush happens
                            try
                            {
                                job.RunBlocking();
                                job.Finish();

                                // Success!
                                break;
                            }
                            catch (IOException ioe)
                            {
                                job.Cancel("startup failed", ioe);
                                if (ioe.Message.Contains("checksum mismatch after file copy"))
                                {
                                    // OK-ish
                                    Message("top: failed to copy: " + ioe + "; retrying");
                                }
                                else
                                {
                                    throw ioe;
                                }
                            }
                        }

                        lastPrimaryGen = job.GetCopyState().primaryGen;

                        SegmentInfos syncInfos =
                            SegmentInfos.ReadCommit(
                                dir, toIndexInput(job.GetCopyState().infosBytes), job.GetCopyState().gen);

                        // Must always commit to a larger generation than what's currently in the index:
                        syncInfos.UpdateGeneration(infos);
                        infos = syncInfos;
                        if (Debugging.AssertsEnabled)
                        {
                            Debugging.Assert(infos.Version == job.GetCopyState().version);
                        }
                        Message("  version=" + infos.Version + " segments=" + infos.ToString());
                        Message("top: init: incRef nrtFiles=" + job.GetFileNames());
                        deleter.IncRef(job.GetFileNames());
                        Message("top: init: decRef lastNRTFiles=" + lastNRTFiles);
                        deleter.DecRef(lastNRTFiles);

                        lastNRTFiles.Clear();
                        lastNRTFiles.AddRange(job.GetFileNames());

                        Message("top: init: set lastNRTFiles=" + lastNRTFiles);
                        lastFileMetaData = job.GetCopyState().files;
                        Message(
                            string.Format(
                                "top: %d: start: done sync: took %.3fs for %s, opened NRT reader version=%d",
                                id,
                                (Time.NanoTime() - initSyncStartNS) / (double)Extensions.TimeUnitSecondsToNanos(1),
                                BytesToString(job.GetTotalBytesCopied()),
                                job.GetCopyState().version));

                        doCommit = true;
                    }
                    else
                    {
                        doCommit = false;
                        lastPrimaryGen = curPrimaryGen;
                        Message("top: same primary as before");
                    }

                    if (infos.Generation < maxPendingGen)
                    {
                        Message(
                            "top: move infos generation from " + infos.Generation + " to " + maxPendingGen);
                        infos.SetNextWriteGeneration(maxPendingGen);
                    }

                    // Notify primary we started, to give it a chance to send any warming merges our way to reduce
                    // NRT latency of first sync:
                    SendNewReplica();

                    // Finally, we are open for business, since our index now "agrees" with the primary:
                    mgr = new SegmentInfosSearcherManager(dir, this, infos, searcherFactory);

                    // Must commit after init mgr:
                    if (doCommit)
                    {
                        // Very important to commit what we just sync'd over, because we removed the pre-existing
                        // commit point above if we had to
                        // overwrite any files it referenced:
                        Commit();
                    }

                    Message("top: done start");
                    state = "idle";
                }
                catch (Exception t)
                {
                    if (t.Message.StartsWith("replica cannot start") == false)
                    {
                        Message("exc on start:");
                        t.PrintStackTrace(TextWriter);
                    }
                    else
                    {
                        dir.Dispose();
                    }
                    throw IOUtils.RethrowAlways(t);
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }

        }

        readonly object commitLock = new object();

        /// <exception cref="IOException"/>
        public override void Commit()
        {
            UninterruptableMonitor.Enter(commitLock);
            try
            {
                SegmentInfos infos;
                Collection<String> indexFiles;
                UninterruptableMonitor.Enter(this);
                try
                {
                    infos = ((SegmentInfosSearcherManager)mgr).GetCurrentInfos();
                    indexFiles = infos.Files(false);
                    deleter.IncRef(indexFiles);
                }
                finally
                {
                    UninterruptableMonitor.Exit(this);
                }
                Message(
                   "top: commit primaryGen="
                       + lastPrimaryGen
                       + " infos="
                       + infos.ToString()
                       + " files="
                       + indexFiles);

                // fsync all index files we are now referencing
                dir.Sync(indexFiles);

                IDictionary<string, string> commitData = new Dictionary<string, string>();
                commitData.Add(PRIMARY_GEN_KEY, lastPrimaryGen.ToString());
                commitData.Add(VERSION_KEY, GetCurrentSearchingVersion().ToString());
                infos.SetUserData(commitData, false);

                // write and fsync a new segments_N
                infos.Commit(dir);

                // Notify current infos (which may have changed while we were doing dir.sync above) what
                // generation we are up to; this way future
                // commits are guaranteed to go to the next (unwritten) generations:
                ((SegmentInfosSearcherManager)mgr).GetCurrentInfos().UpdateGeneration(infos);
                String segmentsFileName = infos.GetSegmentsFileName();
                Message(
                    "top: commit wrote segments file "
                        + segmentsFileName
                        + " version="
                        + infos.Version
                        + " sis="
                        + infos.ToString()
                        + " commitData="
                        + commitData);
                deleter.IncRef(new List<string>()
                {
                    segmentsFileName
                });
                Message("top: commit decRef lastCommitFiles=" + lastCommitFiles);
                deleter.DecRef(lastCommitFiles);
                lastCommitFiles.Clear();
                lastCommitFiles.AddRange(indexFiles);
                lastCommitFiles.Add(segmentsFileName);
                Message("top: commit version=" + infos.Version + " files now " + lastCommitFiles);
            }
            finally
            {
                UninterruptableMonitor.Exit(commitLock);
            }
        }

        /// <exception cref="IOException"/>
        protected void FinishNRTCopy(CopyJob job, long startNS)
        {
            CopyState copyState = job.GetCopyState();
            Message(
                "top: finishNRTCopy: version="
                    + copyState.version
                    + (job.GetFailed() ? " FAILED" : "")
                    + " job="
                    + job);

            // NOTE: if primary crashed while we were still copying then the job will hit an exc trying to
            // read bytes for the files from the primary node,
            // and the job will be marked as failed here:
            UninterruptableMonitor.Enter(this);
            try
            {
                if ("syncing".Equals(state))
                {
                    state = "idle";
                }

                if (curNRTCopy == job)
                {
                    Message("top: now clear curNRTCopy; job=" + job);
                    curNRTCopy = null;
                }
                else
                {
                    if (Debugging.AssertsEnabled)
                    {
                        Debugging.Assert(job.GetFailed());
                    }
                    Message("top: skip clear curNRTCopy: we were cancelled; job=" + job);
                }

                if (job.GetFailed())
                {
                    return;
                }

                // Does final file renames:
                job.Finish();

                // Turn byte[] back to SegmentInfos:
                SegmentInfos infos =
                    SegmentInfos.ReadCommit(dir, toIndexInput(copyState.infosBytes), copyState.gen);
                if (Debugging.AssertsEnabled)
                {
                    Debugging.Assert(infos.Version == copyState.version);
                }

                Message("  version=" + infos.Version + " segments=" + infos.ToString());

                // Cutover to new searcher:
                ((SegmentInfosSearcherManager)mgr).SetCurrentInfos(infos);

                // Must first incRef new NRT files, then decRef old ones, to make sure we don't remove an NRT
                // file that's in common to both:
                ICollection<string> newFiles = copyState.files.Keys.ToList();
                Message("top: incRef newNRTFiles=" + newFiles);
                deleter.IncRef(newFiles);

                // If any of our new files were previously copied merges, we clear them now, so we don't try
                // to later delete a non-existent file:
                pendingMergeFiles.RemoveAll(newFiles);
                Message("top: after remove from pending merges pendingMergeFiles=" + pendingMergeFiles);

                Message("top: decRef lastNRTFiles=" + lastNRTFiles);
                deleter.DecRef(lastNRTFiles);
                lastNRTFiles.Clear();
                lastNRTFiles.AddRange(newFiles);
                Message("top: set lastNRTFiles=" + lastNRTFiles);

                // At this point we can remove any completed merge segment files that we still do not
                // reference.  This can happen when a merge
                // finishes, copies its files out to us, but is then merged away (or dropped due to 100%
                // deletions) before we ever cutover to it
                // in an NRT point:
                if (copyState.completedMergeFiles.Any())
                {
                    Message("now remove-if-not-ref'd completed merge files: " + copyState.completedMergeFiles);
                    foreach (string fileName in copyState.completedMergeFiles)
                    {
                        if (pendingMergeFiles.Contains(fileName))
                        {
                            pendingMergeFiles.Remove(fileName);
                            deleter.DeleteIfNoRef(fileName);
                        }
                    }
                }

                lastFileMetaData = copyState.files;
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }

            int markerCount;
            IndexSearcher s = mgr.Acquire();
            try
            {
                markerCount = s.Count(new TermQuery(new Term("marker", "marker")));
            }
            finally
            {
                mgr.Release(s);
            }

            Message(
                String.Format(
                    "top: done sync: took %.3fs for %s, opened NRT reader version=%d markerCount=%d",
                    (Time.NanoTime() - startNS) / (double)Extensions.TimeUnitSecondsToNanos(1),
                    BytesToString(job.GetTotalBytesCopied()),
                    copyState.version,
                    markerCount));
        }

        private ChecksumIndexInput toIndexInput(byte[] input)
        {
            return new BufferedChecksumIndexInput(
                new ByteBufferIndexInput(
                    new ByteBufferDataInput(Arrays.AsList(ByteBuffer.wrap(input))), "SegmentInfos"));
        }

        /**
         * Start a background copying job, to copy the specified files from the current primary node. If
         * files is null then the latest copy state should be copied. If prevJob is not null, then the new
         * copy job is replacing it and should 1) cancel the previous one, and 2) optionally salvage e.g.
         * partially copied and, shared with the new copy job, files.
         */
        /// <exception cref="IOException"/>
        protected abstract CopyJob NewCopyJob(
            String reason,
            IDictionary<String, FileMetaData> files,
            IDictionary<String, FileMetaData> prevFiles,
            bool highPriority,
            CopyJob.IOnceDone onceDone);

        /** Runs this job async'd */
        protected abstract void Launch(CopyJob job);

        /**
         * Tell primary we (replica) just started, so primary can tell us to warm any already warming
         * merges. This lets us keep low nrt refresh time for the first nrt sync after we started.
         */
        /// <exception cref="IOException"/>
        protected abstract void SendNewReplica();

        /**
         * Call this to notify this replica node that a new NRT infos is available on the primary. We kick
         * off a job (runs in the background) to copy files across, and open a new reader once that's
         * done.
         */
        /// <exception cref="IOException"/>
        public CopyJob NewNRTPoint(long newPrimaryGen, long version)
        {
            //sync
            UninterruptableMonitor.Enter(this);
            try
            {

                if (IsClosed())
                {
                    throw AlreadyClosedException.Create("this replica is closed: state=" + state);
                }

                // Cutover (possibly) to new primary first, so we discard any pre-copied merged segments up
                // front, before checking for which files need
                // copying.  While it's possible the pre-copied merged segments could still be useful to us, in
                // the case that the new primary is either
                // the same primary (just e.g. rebooted), or a promoted replica that had a newer NRT point than
                // we did that included the pre-copied
                // merged segments, it's still a bit risky to rely solely on checksum/file length to catch the
                // difference, so we defensively discard
                // here and re-copy in that case:
                MaybeNewPrimary(newPrimaryGen);

                // Caller should not "publish" us until we have finished .start():
                if (Debugging.AssertsEnabled)
                {
                    Debugging.Assert(mgr != null);
                }

                if ("idle".Equals(state))
                {
                    state = "syncing";
                }

                long curVersion = GetCurrentSearchingVersion();

                Message("top: start sync sis.version=" + version);

                if (version == curVersion)
                {
                    // Caller releases the CopyState:
                    Message("top: new NRT point has same version as current; skipping");
                    return null;
                }

                if (version < curVersion)
                {
                    // This can happen, if two syncs happen close together, and due to thread scheduling, the
                    // incoming older version runs after the newer version
                    Message(
                        "top: new NRT point (version="
                            + version
                            + ") is older than current (version="
                            + curVersion
                            + "); skipping");
                    return null;
                }

                long startNS = Time.NanoTime();

                Message("top: newNRTPoint");
                CopyJob job = null;
                try
                {
                    job =
                        NewCopyJob(
                            "NRT point sync version=" + version,
                            null,
                            lastFileMetaData,
                            true,
                            new CopyJob.OnceDoneAction((job) =>
                            {

                                try
                                {
                                    FinishNRTCopy(job, startNS);
                                }
                                catch (IOException ioe)
                                {
                                    throw RuntimeException.Create(ioe);
                                }
                            }));
                }
                catch (NodeCommunicationException nce)
                {
                    // E.g. primary could crash/close when we are asking it for the copy state:
                    Message("top: ignoring communication exception creating CopyJob: " + nce);
                    // nce.printStackTrace(printStream);
                    if (state.Equals("syncing"))
                    {
                        state = "idle";
                    }
                    return null;
                }
                if (Debugging.AssertsEnabled)
                {
                    Debugging.Assert(newPrimaryGen == job.GetCopyState().primaryGen);
                }

                ICollection<string> newNRTFiles = job.GetFileNames();

                Message("top: newNRTPoint: job files=" + newNRTFiles);

                if (curNRTCopy != null)
                {
                    job.TransferAndCancel(curNRTCopy);
                    if (Debugging.AssertsEnabled)
                    {
                        Debugging.Assert(curNRTCopy.GetFailed());
                    }
                }

                curNRTCopy = job;

                foreach (string fileName in curNRTCopy.GetFileNamesToCopy())
                {
                    if (Debugging.AssertsEnabled)
                    {
                        Debugging.Assert(lastCommitFiles.Contains(fileName) == false, "fileName=" + fileName + " is in lastCommitFiles and is being copied?");
                    }
                    UninterruptableMonitor.Enter(mergeCopyJobs);
                    try
                    {
                        foreach (CopyJob mergeJob in mergeCopyJobs)
                        {
                            if (mergeJob.GetFileNames().Contains(fileName))
                            {
                                // TODO: we could maybe transferAndCancel here?  except CopyJob can't transferAndCancel
                                // more than one currently
                                Message(
                                    "top: now cancel merge copy job="
                                        + mergeJob
                                        + ": file "
                                        + fileName
                                        + " is now being copied via NRT point");
                                mergeJob.Cancel("newNRTPoint is copying over the same file", null);
                            }
                        }
                    }
                    finally
                    {
                        UninterruptableMonitor.Exit(mergeCopyJobs);
                    }

                }

                try
                {
                    job.Start();
                }
                catch (NodeCommunicationException nce)
                {
                    // E.g. primary could crash/close when we are asking it for the copy state:
                    Message("top: ignoring exception starting CopyJob: " + nce);
                    nce.PrintStackTrace(printStream);
                    if (state.Equals("syncing"))
                    {
                        state = "idle";
                    }
                    return null;
                }

                // Runs in the background jobs thread, maybe slowly/throttled, and calls finishSync once it's
                // done:
                Launch(curNRTCopy);
                return curNRTCopy;
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }

        public bool IsCopying()
        {
            UninterruptableMonitor.Enter(this);
            try
            {

                return curNRTCopy != null;
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }

        public override bool IsClosed()
        {
            return "closed".Equals(state)
                || "closing".Equals(state)
                || "crashing".Equals(state)
                || "crashed".Equals(state);
        }

        /// <exception cref="IOException"/>
        public override void Dispose()
        {
            Message("top: now close");
            UninterruptableMonitor.Enter(this);
            try
            {
                state = "closing";
                if (curNRTCopy != null)
                {
                    curNRTCopy.Cancel("closing", null);
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
            UninterruptableMonitor.Enter(this);
            try
            {
                Message("top: close mgr");
                mgr.Dispose();

                Message("top: decRef lastNRTFiles=" + lastNRTFiles);
                deleter.DecRef(lastNRTFiles);
                lastNRTFiles.Clear();

                // NOTE: do not decRef these!
                lastCommitFiles.Clear();

                Message("top: delete if no ref pendingMergeFiles=" + pendingMergeFiles);
                foreach (String fileName in pendingMergeFiles)
                {
                    deleter.DeleteIfNoRef(fileName);
                }
                pendingMergeFiles.Clear();

                Message("top: close dir");
                IOUtils.Close(writeFileLock, dir);
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
            Message("top: done close");
            state = "closed";
        }

        /** Called when the primary changed */
        /// <exception cref="IOException"/>
        protected void MaybeNewPrimary(long newPrimaryGen)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                if (newPrimaryGen != lastPrimaryGen)
                {
                    Message(
                        "top: now change lastPrimaryGen from "
                            + lastPrimaryGen
                            + " to "
                            + newPrimaryGen
                            + " pendingMergeFiles="
                            + pendingMergeFiles);

                    Message("top: delete if no ref pendingMergeFiles=" + pendingMergeFiles);
                    foreach (String fileName in pendingMergeFiles)
                    {
                        deleter.DeleteIfNoRef(fileName);
                    }
                    if (Debugging.AssertsEnabled)
                    {
                        Debugging.Assert(newPrimaryGen > lastPrimaryGen, "newPrimaryGen=" + newPrimaryGen + " vs lastPrimaryGen=" + lastPrimaryGen);
                    }
                    lastPrimaryGen = newPrimaryGen;
                    pendingMergeFiles.Clear();
                }
                else
                {
                    Message("top: keep current lastPrimaryGen=" + lastPrimaryGen);
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }

        }
        /// <exception cref="IOException"/>
        protected CopyJob LaunchPreCopyMerge(AtomicBoolean finished, long newPrimaryGen, IDictionary<string, FileMetaData> files)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                CopyJob job;

                MaybeNewPrimary(newPrimaryGen);
                long primaryGenStart = lastPrimaryGen;
                ISet<string> fileNames = files.Keys.ToHashSet();
                Message("now pre-copy warm merge files=" + fileNames + " primaryGen=" + newPrimaryGen);

                foreach (string fileName in fileNames)
                {
                    if (Debugging.AssertsEnabled)
                    {
                        Debugging.Assert(pendingMergeFiles.Contains(fileName) == false, "file \"" + fileName + "\" is already being warmed!");
                        Debugging.Assert(lastNRTFiles.Contains(fileName) == false, "file \"" + fileName + "\" is already NRT visible!");
                    }
                }

                job =
                    NewCopyJob(
                        "warm merge on " + Name() + " filesNames=" + fileNames,
                        files,
                        null,
                        false,
                        new CopyJob.OnceDoneAction(
                            (job) =>
                        {
                            // Signals that this replica has finished
                            mergeCopyJobs.Remove(job);
                            Message("done warming merge " + fileNames + " failed?=" + job.GetFailed());
                            UninterruptableMonitor.Enter(this);
                            try
                            {
                                if (job.GetFailed() == false)
                                {
                                    if (lastPrimaryGen != primaryGenStart)
                                    {
                                        Message(
                                            "merge pre copy finished but primary has changed; cancelling job files="
                                                + fileNames);
                                        job.Cancel("primary changed during merge copy", null);
                                    }
                                    else
                                    {
                                        bool abort = false;
                                        foreach (string fileName in fileNames)
                                        {
                                            if (lastNRTFiles.Contains(fileName))
                                            {
                                                Message(
                                                    "abort merge finish: file "
                                                        + fileName
                                                        + " is referenced by last NRT point");
                                                abort = true;
                                            }
                                            if (lastCommitFiles.Contains(fileName))
                                            {
                                                Message(
                                                    "abort merge finish: file "
                                                        + fileName
                                                        + " is referenced by last commit point");
                                                abort = true;
                                            }
                                        }
                                        if (abort)
                                        {
                                            // Even though in newNRTPoint we have similar logic, which cancels any merge
                                            // copy jobs if an NRT point
                                            // shows up referencing the files we are warming (because primary got
                                            // impatient and gave up on us), we also
                                            // need it here in case replica is way far behind and fails to even receive
                                            // the merge pre-copy request
                                            // until after the newNRTPoint referenced those files:
                                            job.Cancel("merged segment was separately copied via NRT point", null);
                                        }
                                        else
                                        {
                                            job.Finish();
                                            Message("merge pre copy finished files=" + fileNames);
                                            foreach (String fileName in fileNames)
                                            {
                                                if (Debugging.AssertsEnabled)
                                                {
                                                    Debugging.Assert(pendingMergeFiles.Contains(fileName) == false, "file \"" + fileName + "\" is already in pendingMergeFiles");
                                                }
                                                Message("add file " + fileName + " to pendingMergeFiles");
                                                pendingMergeFiles.Add(fileName);
                                            }
                                        }
                                    }
                                }
                                else
                                {
                                    Message("merge copy finished with failure");
                                }
                            }
                            finally
                            {
                                UninterruptableMonitor.Exit(this);
                            }
                            finished.GetAndSet(true);
                        }
                        ));

                job.Start();

                // When warming a merge we better not already have any of these files copied!
                if (Debugging.AssertsEnabled)
                {
                    Debugging.Assert(job.GetFileNamesToCopy().Count == files.Count);
                }

                mergeCopyJobs.Add(job);
                Launch(job);

                return job;
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }


        /// <exception cref="IOException"/>
        public IndexOutput CreateTempOutput(String prefix, String suffix, IOContext ioContext)
        {
            return dir.CreateTempOutput(prefix, suffix, IOContext.DEFAULT);
        }

        /// <summary>
        /// Compares incoming per-file identity (id, checksum, header, footer) versus what we have locally
        /// and returns the subset of the incoming files that need copying
        /// </summary>
        /// <exception cref="IOException"/>
        public List<KeyValuePair<String, FileMetaData>> GetFilesToCopy(IDictionary<String, FileMetaData> files)
        {

            List<KeyValuePair<String, FileMetaData>> toCopy = new List<KeyValuePair<String, FileMetaData>>();
            foreach (KeyValuePair<String, FileMetaData> ent in files)
            {
                String fileName = ent.Key;
                FileMetaData fileMetaData = ent.Value;
                if (FileIsIdentical(fileName, fileMetaData) == false)
                {
                    toCopy.Add(ent);
                }
            }

            return toCopy;
        }

        /// <summary>
        /// Carefully determine if the file on the primary, identified by its {@code String fileName} along
        /// with the {@link FileMetaData} "summarizing" its contents, is precisely the same file that we
        /// have locally. If the file does not exist locally, or if its header (includes the segment id),
        /// length, footer (including checksum) differ, then this returns false, else true.
        /// </summary>
        /// <exception cref="IOException"/>
        private bool FileIsIdentical(String fileName, FileMetaData srcMetaData)
        {

            FileMetaData destMetaData = ReadLocalFileMetaData(fileName);
            if (destMetaData == null)
            {
                // Something went wrong in reading the file (it's corrupt, truncated, does not exist, etc.):
                return false;
            }

            if (Arrays.Equals(destMetaData.header, srcMetaData.header) == false
                || Arrays.Equals(destMetaData.footer, srcMetaData.footer) == false)
            {
                // Segment name was reused!  This is rare but possible and otherwise devastating:
                if (Node.VERBOSE_FILES)
                {
                    Message("file " + fileName + ": will copy [header/footer is different]");
                }
                return false;
            }
            else
            {
                return true;
            }
        }

        private ConcurrentDictionary<string, bool> copying = new ConcurrentDictionary<string, bool>();

        // Used only to catch bugs, ensuring a given file name is only ever being copied bye one job:
        public void StartCopyFile(String name)
        {
            if (copying.PutIfAbsent(name, Boolean.TRUE) != null)
            {
                throw IllegalStateException.Create("file " + name + " is being copied in two places!");
            }
        }

        public void FinishCopyFile(String name)
        {
            if (copying.Remove(name) == null)
            {
                throw IllegalStateException.Create("file " + name + " was not actually being copied?");
            }
        }
    }
}
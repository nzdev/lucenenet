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

using J2N;
using J2N.Threading.Atomic;
using Lucene.Net.Diagnostics;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Support;
using Lucene.Net.Support.Threading;
using Lucene.Net.Util;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Lucene.Net.Replicator.Nrt
{

    /*
     * This just asks IndexWriter to open new NRT reader, in order to publish a new NRT point.  This could be improved, if we separated out 1)
     * nrt flush (and incRef the SIS) from 2) opening a new reader, but this is tricky with IW's concurrency, and it would also be hard-ish to share
     * IW's reader pool with our searcher manager.  So we do the simpler solution now, but that adds some unnecessary latency to NRT refresh on
     * replicas since step 2) could otherwise be done concurrently with replicas copying files over.
     */

    /// <summary>
    /// Node that holds an IndexWriter, indexing documents into its local index.
    /// </summary>
    /// <remarks>
    /// @lucene.experimental
    /// </remarks>
    public abstract class PrimaryNode : Node
    {

        // Current NRT segment infos, incRef'd with IndexWriter.deleter:
        private SegmentInfos curInfos;

        protected readonly IndexWriter writer;

        // IncRef'd state of the last published NRT point; when a replica comes asking, we give it this as
        // the current NRT point:
        private CopyState copyState;

        protected readonly long primaryGen;
        private int remoteCloseTimeoutMs = -1;

        /// <summary>
        ///  Contains merged segments that have been copied to all running replicas (as of when that merge started warming).
        /// </summary>
        internal readonly ISet<string> finishedMergedFiles = new ConcurrentHashSet<string>();

        private readonly AtomicInt32 copyingCount = new AtomicInt32();

        /// <exception cref="IOException"/>
        public PrimaryNode(
            IndexWriter writer,
            int id,
            long primaryGen,
            long forcePrimaryVersion,
            SearcherFactory searcherFactory,
            TextWriter TextWriter) : base(id, writer.Directory, searcherFactory, TextWriter)
        {
            Message("top: now init primary");
            this.writer = writer;
            this.primaryGen = primaryGen;

            try
            {
                // So that when primary node's IndexWriter finishes a merge, but before it cuts over to the
                // merged segment,
                // it copies it out to the replicas.  This ensures the whole system's NRT latency remains low
                // even when a
                // large merge completes:
                writer.Config.MergedSegmentWarmer = new PreCopyMergedSegmentWarmer(this);

                Message("IWC:\n" + writer.Config);
                Message("dir:\n" + writer.Directory);
                Message("commitData: " + writer.GetLiveCommitData());

                // Record our primaryGen in the userData, and set initial version to 0:
                IDictionary<string, string> commitData = new Dictionary<string, string>();
                IEnumerable<KeyValuePair<string, string>> iter = writer.GetLiveCommitData();
                if (iter != null)
                {
                    foreach (KeyValuePair<string, string> ent in iter)
                    {
                        commitData.Add(ent.Key, ent.Value);
                    }
                }
                commitData.Add(PRIMARY_GEN_KEY, primaryGen.ToString());
                if (commitData[VERSION_KEY] == null)
                {
                    commitData.Add(VERSION_KEY, "0");
                    Message("add initial commitData version=0");
                }
                else
                {
                    Message("keep current commitData version=" + commitData[VERSION_KEY]);
                }
                writer.SetLiveCommitData(commitData.AsEnumerable(), false);

                // We forcefully advance the SIS version to an unused future version.  This is necessary if
                // the previous primary crashed and we are
                // starting up on an "older" index, else versions can be illegally reused but show different
                // results:
                if (forcePrimaryVersion != -1)
                {
                    Message("now forcePrimaryVersion to version=" + forcePrimaryVersion);
                    writer.AdvanceSegmentInfosVersion(forcePrimaryVersion);
                }

                mgr = new SearcherManager(writer, true, true, searcherFactory);
                SetCurrentInfos(new HashSet<string>());
                Message("init: infos version=" + curInfos.Version);

            }
            catch (Exception t)
            {
                Message("init: exception");
                t.PrintStackTrace(TextWriter);
                throw new RuntimeException(t);
            }
        }

        /// <summary>
        /// Returns the current primary generation, which is incremented each time a new primary is started for this index
        /// </summary>
        public long getPrimaryGen()
        {
            return primaryGen;
        }

        // TODO: in the future, we should separate "flush" (returns an incRef'd SegmentInfos) from
        // "refresh" (open new NRT reader from
        // IndexWriter) so that the latter can be done concurrently while copying files out to replicas,
        // minimizing the refresh time from the
        // replicas.  But fixing this is tricky because e.g. IndexWriter may complete a big merge just
        // after returning the incRef'd SegmentInfos
        // and before we can open a new reader causing us to close the just-merged readers only to then
        // open them again from the (now stale)
        // SegmentInfos.  To fix this "properly" I think IW.inc/decRefDeleter must also incread the
        // ReaderPool entry

        /// <summary>
        /// Flush all index operations to disk and opens a new near-real-time reader. new NRT point, to
        /// make the changes visible to searching. Returns true if there were changes.
        /// </summary>
        /// <exception cref="IOException"/>
        public bool FlushAndRefresh()
        {
            Message("top: now flushAndRefresh");
            ISet<string> completedMergeFiles;
            UninterruptableMonitor.Enter(this);
            try
            {
                completedMergeFiles = Set.copyOf(finishedMergedFiles);
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
            mgr.MaybeRefreshBlocking();
            bool result = SetCurrentInfos(completedMergeFiles);
            if (result)
            {
                Message("top: opened NRT reader version=" + curInfos.Version);
                finishedMergedFiles.RemoveAll(completedMergeFiles);
                Message(
                    "flushAndRefresh: version="
                        + curInfos.Version
                        + " completedMergeFiles="
                        + completedMergeFiles
                        + " finishedMergedFiles="
                        + finishedMergedFiles);
            }
            else
            {
                Message("top: no changes in flushAndRefresh; still version=" + curInfos.Version);
            }
            return result;
        }

        public long GetCopyStateVersion()
        {
            return copyState.version;
        }

        public long GetLastCommitVersion()
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                IEnumerable<KeyValuePair<string, string>> iter = writer.GetLiveCommitData();
                if (Debugging.AssertsEnabled)
                {
                    Debugging.Assert(iter != null);
                }
                foreach (KeyValuePair<string, string> ent in  iter)
                {
                    if (ent.Key.Equals(VERSION_KEY))
                    {
                        return long.Parse(ent.Value);
                    }
                }

                // In ctor we always install an initial version:
                throw AssertionError.Create("missing VERSION_KEY");
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }

        }

        /// <returns>the number of milliseconds to wait during shutdown for remote replicas to close</returns>
        public int getRemoteCloseTimeoutMs()
        {
            return remoteCloseTimeoutMs;
        }

        /// <summary>
        /// Set the number of milliseconds to wait during shutdown for remote replicas to close. {@code -1}
        /// (the default) means forever, and {@code 0} means don't wait at all.
        /// </summary>
        public void setRemoteCloseTimeoutMs(int remoteCloseTimeoutMs)
        {
            if (remoteCloseTimeoutMs < -1)
            {
                throw new IllegalArgumentException("bad timeout + + remoteCloseTimeoutMs");
            }
            this.remoteCloseTimeoutMs = remoteCloseTimeoutMs;
        }


        /// <exception cref="IOException"/>
        public override void Commit()
        {
            IDictionary<string, string> commitData = new Dictionary<string, string>();
            commitData.Add(PRIMARY_GEN_KEY, primaryGen.ToString());
            // TODO (opto): it's a bit wasteful that we put "last refresh" version here, not the actual
            // version we are committing, because it means
            // on xlog replay we are replaying more ops than necessary.
            commitData.Add(VERSION_KEY, copyState.version.ToString());
            Message("top: commit commitData=" + commitData);
            writer.SetLiveCommitData(commitData.AsEnumerable(), false);
            writer.Commit();
        }

        /** IncRef the current CopyState and return it */
        /// <exception cref="IOException"/>
        public CopyState GetCopyState()
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                EnsureOpen(false);
                // message("top: getCopyState replicaID=" + replicaID + " replicaNodeID=" + replicaNodeID + "
                // version=" + curInfos.getVersion() + " infos=" + curInfos.toString());
                if (Debugging.AssertsEnabled)
                {
                    Debugging.Assert(curInfos == copyState.infos);
                }

                writer.IncRefDeleter(copyState.infos);
                int count = copyingCount.IncrementAndGet();
                if (Debugging.AssertsEnabled)
                {
                    Debugging.Assert(count > 0);
                }
                return copyState;
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }


        }

        /** Called once replica is done (or failed) copying an NRT point */
        /// <exception cref="IOException"/>
        public void ReleaseCopyState(CopyState copyState)
        {
            // message("top: releaseCopyState version=" + copyState.version);
            if (Debugging.AssertsEnabled)
            {
                Debugging.Assert(copyState.infos != null);
            }
            writer.DecRefDeleter(copyState.infos);
            int count = copyingCount.DecrementAndGet();
            if (Debugging.AssertsEnabled)
            {
                Debugging.Assert(count >= 0);
            }
        }

        public override bool IsClosed()
        {
            return IsClosed(false);
        }

        bool IsClosed(bool allowClosing)
        {
            return "closed".Equals(state) || (allowClosing == false && "closing".Equals(state));
        }

        private void EnsureOpen(bool allowClosing)
        {
            if (IsClosed(allowClosing))
            {
                throw AlreadyClosedException.Create(state);
            }
        }

        /// <remarks>
        /// Steals incoming infos refCount; returns true if there were changes.
        /// </remarks>
        /// <exception cref="IOException"/>
        private bool SetCurrentInfos(ISet<string> completedMergeFiles)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                IndexSearcher searcher = null;
                SegmentInfos infos;
                try
                {
                    searcher = mgr.Acquire();
                    infos = ((StandardDirectoryReader)searcher.GetIndexReader()).GetSegmentInfos();
                }
                finally
                {
                    if (searcher != null)
                    {
                        mgr.Release(searcher);
                    }
                }
                if (curInfos != null && infos.Version == curInfos.Version)
                {
                    // no change
                    Message(
                        "top: skip switch to infos: version="
                            + infos.Version
                            + " is unchanged: "
                            + infos.ToString());
                    return false;
                }

                SegmentInfos oldInfos = curInfos;
                writer.IncRefDeleter(infos);
                curInfos = infos;
                if (oldInfos != null)
                {
                    writer.DecRefDeleter(oldInfos);
                }

                Message("top: switch to infos=" + infos.ToString() + " version=" + infos.Version);

                // Serialize the SegmentInfos.
                ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();
                using (ByteBuffersIndexOutput tmpIndexOutput =
                    new ByteBuffersIndexOutput(buffer, "temporary", "temporary"))
                {
                    infos.Write(tmpIndexOutput);
                }
                byte[] infosBytes = buffer.toArrayCopy();

                Dictionary<string, FileMetaData> filesMetaData = new Dictionary<string, FileMetaData>();
                foreach (SegmentCommitInfo info in infos)
                {
                    foreach (String fileName in info.GetFiles())
                    {
                        FileMetaData metaData = ReadLocalFileMetaData(fileName);
                        // NOTE: we hold a refCount on this infos, so this file better exist:
                        if (Debugging.AssertsEnabled)
                        {
                            Debugging.Assert(metaData != null, "file \"" + fileName + "\" is missing metadata");
                            Debugging.Assert(filesMetaData.ContainsKey(fileName) == false);
                        }
                        filesMetaData.Add(fileName, metaData);
                    }
                }

                lastFileMetaData = filesMetaData;

                Message(
                    "top: set copyState primaryGen="
                        + primaryGen
                        + " version="
                        + infos.Version
                        + " files="
                        + filesMetaData.Keys);
                copyState =
                    new CopyState(
                        lastFileMetaData,
                        infos.Version,
                        infos.Generation,
                        infosBytes,
                        completedMergeFiles,
                        primaryGen,
                        curInfos);
                return true;
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
            
        }

        /// <exception cref="IOException"/>
        private void WaitForAllRemotesToClose()
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                if (remoteCloseTimeoutMs == 0)
                {
                    return;
                }
                long waitStartNs = Time.NanoTime();
                // Wait for replicas to finish or crash or timeout:
                while (remoteCloseTimeoutMs < 0
                    || (Time.NanoTime() - waitStartNs) / 1_000_000 < remoteCloseTimeoutMs)
                {
                    int count = copyingCount.Value;
                    if (count == 0)
                    {
                        return;
                    }
                    Message("pendingCopies: " + count);

                    try
                    {
                        Wait(10);
                    }
                    catch (InterruptedException ie)
                    {
                        throw new ThreadInterruptedException(ie);
                    }
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }

        }

        /// <exception cref="IOException"/>
        public override void Dispose()
        {
            state = "closing";
            Message("top: close primary");

            lock (this)
            {
                WaitForAllRemotesToClose();
                if (curInfos != null)
                {
                    writer.DecRefDeleter(curInfos);
                    curInfos = null;
                }
            }

            mgr.Close();

            writer.Rollback();
            dir.Close();

            state = "closed";
        }

        /// <summary>
        /// Called when a merge has finished, but before IW switches to the merged segment
        /// </summary>
        /// <exception cref="IOException"/>
        internal abstract void PreCopyMergedSegmentFiles(
            SegmentCommitInfo info, IDictionary<string, FileMetaData> files);
    }
}

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


using J2N.Threading.Atomic;
using Lucene.Net.Diagnostics;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Support.Threading;
using Lucene.Net.Util;
using System;
using System.Collections.Generic;
using System.IO;
using JCG = J2N.Collections.Generic;

namespace Lucene.Net.Replicator.Nrt
{
    /*
 * This just asks IndexWriter to open new NRT reader, in order to publish a new NRT point.  This could be improved, if we separated out 1)
 * nrt flush (and incRef the SIS) from 2) opening a new reader, but this is tricky with IW's concurrency, and it would also be hard-ish to share
 * IW's reader pool with our searcher manager.  So we do the simpler solution now, but that adds some unecessary latency to NRT refresh on
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

        readonly IndexWriter writer;

        // IncRef'd state of the last published NRT point; when a replica comes asking, we give it this as the current NRT point:
        private CopyState copyState;

        protected readonly long primaryGen;

        /** Contains merged segments that have been copied to all running replicas (as of when that merge started warming). */
        internal readonly ISet<string> finishedMergedFiles = Collections.synchronizedSet(new HashSet<string>());

        private readonly AtomicInt32 copyingCount = new AtomicInt32();

        /// <exception cref="IOException"/>
        public PrimaryNode(IndexWriter writer, int id, long primaryGen, long forcePrimaryVersion,
                           SearcherFactory searcherFactory, TextWriter printStream)
                  : base(id, writer.Directory, searcherFactory, printStream)
        {
            Message("top: now init primary");
            this.writer = writer;
            this.primaryGen = primaryGen;

            try
            {
                // So that when primary node's IndexWriter finishes a merge, but before it cuts over to the merged segment,
                // it copies it out to the replicas.  This ensures the whole system's NRT latency remains low even when a
                // large merge completes:
                writer.Config.MergedSegmentWarmer = new PreCopyMergedSegmentWarmer(this);

                Message("IWC:\n" + writer.Config);
                Message("dir:\n" + writer.Directory);
                Message("commitData: " + writer.CommitData);

                // Record our primaryGen in the userData, and set initial version to 0:
                IDictionary<string, string> commitData = new Dictionary<string, string>(writer.CommitData);
                commitData.Add(PRIMARY_GEN_KEY, Long.toString(primaryGen));
                if (commitData[VERSION_KEY] == null)
                {
                    commitData.Add(VERSION_KEY, "0");
                    Message("add initial commitData version=0");
                }
                else
                {
                    Message("keep current commitData version=" + commitData[VERSION_KEY]);
                }
                writer.SetCommitData(commitData, false);

                // We forcefully advance the SIS version to an unused future version.  This is necessary if the previous primary crashed and we are
                // starting up on an "older" index, else versions can be illegally reused but show different results:
                if (forcePrimaryVersion != -1)
                {
                    Message("now forcePrimaryVersion to version=" + forcePrimaryVersion);
                    writer.AdvanceSegmentInfosVersion(forcePrimaryVersion);
                }

                mgr = new SearcherManager(writer, true, true, searcherFactory);
                SetCurrentInfos(new JCG.HashSet<string>());
                Message("init: infos version=" + curInfos.Version);

                IndexSearcher s = mgr.Acquire();
                try
                {
                    // TODO: this is test code specific!!
                    Message("init: marker count: " + s.Count(new TermQuery(new Term("marker", "marker"))));
                }
                finally
                {
                    mgr.Release(s);
                }

            }
            catch (Exception t)
            {
                Message("init: exception");
                printStream.Write(t.StackTrace);
                throw RuntimeException.Create(t);
            }
        }

        // TODO: in the future, we should separate "flush" (returns an incRef'd SegmentInfos) from "refresh" (open new NRT reader from
        // IndexWriter) so that the latter can be done concurrently while copying files out to replicas, minimizing the refresh time from the
        // replicas.  But fixing this is tricky because e.g. IndexWriter may complete a big merge just after returning the incRef'd SegmentInfos
        // and before we can open a new reader causing us to close the just-merged readers only to then open them again from the (now stale)
        // SegmentInfos.  To fix this "properly" I think IW.inc/decRefDeleter must also incread the ReaderPool entry

        /// <summary>
        /// Flush all index operations to disk and opens a new near-real-time reader. new NRT point, to
        /// make the changes visible to searching. Returns true if there were changes.
        /// </summary>
        /// <exception cref="IOException"/>
        public bool FlushAndRefresh()
        {
            Message("top: now flushAndRefresh");
            ISet<String> completedMergeFiles;
            UninterruptableMonitor.Enter(finishedMergedFiles);
            try
            {
                completedMergeFiles = Collections.unmodifiableSet(new JCG.HashSet<string>(finishedMergedFiles));
            }
            finally
            {
                UninterruptableMonitor.Exit(finishedMergedFiles);
            }
            mgr.MaybeRefreshBlocking();
            bool result = SetCurrentInfos(completedMergeFiles);
            if (result)
            {
                Message("top: opened NRT reader version=" + curInfos.Version);
                finishedMergedFiles.RemoveAll(completedMergeFiles);
                Message("flushAndRefresh: version=" + curInfos.Version + " completedMergeFiles=" + completedMergeFiles + " finishedMergedFiles=" + finishedMergedFiles);
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
                string s = curInfos.UserData[VERSION_KEY];
                // In ctor we always install an initial version:
                if (Debugging.AssertsEnabled)
                {
                    Debugging.Assert(s != null);
                }

                return Long.parseLong(s);
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }

        }

        /// <exception cref="IOException"/>
        public override void Commit()
        {
            IDictionary<string, string> commitData = new Dictionary<string, string>();
            commitData.Add(PRIMARY_GEN_KEY, Long.toString(primaryGen));
            // TODO (opto): it's a bit wasteful that we put "last refresh" version here, not the actual version we are committing, because it means
            // on xlog replay we are replaying more ops than necessary.
            commitData.Add(VERSION_KEY, Long.toString(copyState.version));
            Message("top: commit commitData=" + commitData);
            writer.SetCommitData(commitData, false);
            writer.Commit();
        }

        /// <summary>
        /// IncRef the current CopyState and return it
        /// </summary>
        /// <exception cref="IOException"/>
        public CopyState GetCopyState()
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                EnsureOpen(false);
                //message("top: getCopyState replicaID=" + replicaID + " replicaNodeID=" + replicaNodeID + " version=" + curInfos.getVersion() + " infos=" + curInfos.toString());
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
        /// <exception cref="System.IO.IOException"/>
        public void ReleaseCopyState(CopyState copyState)
        {
            //message("top: releaseCopyState version=" + copyState.version);
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
        private bool SetCurrentInfos(ISet<String> completedMergeFiles)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                IndexSearcher searcher = null;
                SegmentInfos infos;
                try
                {
                    searcher = mgr.Acquire();
                    infos = ((StandardDirectoryReader)searcher.GetIndexReader()).getSegmentInfos();
                    // TODO: this is test code specific!!
                    Message("setCurrentInfos: marker count: " + searcher.Count(new TermQuery(new Term("marker", "marker"))) + " version=" + infos.Version + " searcher=" + searcher);
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
                    Message("top: skip switch to infos: version=" + infos.Version + " is unchanged: " + infos.ToString());
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

                // Serialize the SegmentInfos:
                RAMOutputStream @out = new RAMOutputStream(new RAMFile(), true);
                infos.Write(dir, @out);
                byte[] infosBytes = new byte[(int)@out.GetFilePointer()];
                @out.WriteTo(infosBytes, 0);

                IDictionary<String, FileMetaData> filesMetaData = new Dictionary<String, FileMetaData>();
                foreach (SegmentCommitInfo info in infos)
                {
                    foreach (String fileName in info.GetFiles())
                    {
                        FileMetaData metaData = ReadLocalFileMetaData(fileName);
                        // NOTE: we hold a refCount on this infos, so this file better exist:
                        if (Debugging.AssertsEnabled)
                        {
                            Debugging.Assert(metaData != null);
                            Debugging.Assert(filesMetaData.ContainsKey(fileName) == false);
                        }
                        filesMetaData.Add(fileName, metaData);
                    }
                }

                lastFileMetaData = Collections.unmodifiableMap(filesMetaData);

                Message("top: set copyState primaryGen=" + primaryGen + " version=" + infos.Version + " files=" + filesMetaData.Keys);
                copyState = new CopyState(lastFileMetaData,
                                          infos.Version, infos.Generation, infosBytes, completedMergeFiles,
                                          primaryGen, curInfos);
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
                // Wait for replicas to finish or crash:
                while (true)
                {
                    int count = copyingCount.Get();
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

        /// <exception cref="System.IO.IOException"/>
        public override void Close()
        {
            state = "closing";
            Message("top: close primary");
            UninterruptableMonitor.Enter(this);
            try
            {
                WaitForAllRemotesToClose();
                if (curInfos != null)
                {
                    writer.DecRefDeleter(curInfos);
                    curInfos = null;
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }

            mgr.Dispose();

            writer.Rollback();
            dir.Dispose();

            state = "closed";
        }

        /** Called when a merge has finished, but before IW switches to the merged segment */
        /// <exception cref="IOException"/>
        protected abstract void PreCopyMergedSegmentFiles(SegmentCommitInfo info, IDictionary<String, FileMetaData> files);
    }
}

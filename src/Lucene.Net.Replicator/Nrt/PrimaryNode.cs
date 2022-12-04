﻿/*
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
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Support.Threading;
using Lucene.Net.Util;
//import java.io.IOException;
//import java.io.TextWriter;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.atomic.AtomicInteger;

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
        readonly Set<String> finishedMergedFiles = Collections.synchronizedSet(new HashSet<String>());

        private readonly AtomicInt32 copyingCount = new AtomicInt32();

        /// <exception cref="IOException"/>
        public PrimaryNode(
            IndexWriter writer,
            int id,
            long primaryGen,
            long forcePrimaryVersion,
            SearcherFactory searcherFactory,
            TextWriter TextWriter)
        {
            super(id, writer.getDirectory(), searcherFactory, TextWriter);
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
                writer.getConfig().setMergedSegmentWarmer(new PreCopyMergedSegmentWarmer(this));

                message("IWC:\n" + writer.getConfig());
                message("dir:\n" + writer.getDirectory());
                message("commitData: " + writer.getLiveCommitData());

                // Record our primaryGen in the userData, and set initial version to 0:
                Map<String, String> commitData = new HashMap<>();
                Iterable<Map.Entry<String, String>> iter = writer.getLiveCommitData();
                if (iter != null)
                {
                    for (Map.Entry<String, String> ent : iter)
                    {
                        commitData.put(ent.getKey(), ent.getValue());
                    }
                }
                commitData.put(PRIMARY_GEN_KEY, Long.toString(primaryGen));
                if (commitData.get(VERSION_KEY) == null)
                {
                    commitData.put(VERSION_KEY, "0");
                    Message("add initial commitData version=0");
                }
                else
                {
                    message("keep current commitData version=" + commitData.get(VERSION_KEY));
                }
                writer.setLiveCommitData(commitData.entrySet(), false);

                // We forcefully advance the SIS version to an unused future version.  This is necessary if
                // the previous primary crashed and we are
                // starting up on an "older" index, else versions can be illegally reused but show different
                // results:
                if (forcePrimaryVersion != -1)
                {
                    Message("now forcePrimaryVersion to version=" + forcePrimaryVersion);
                    writer.advanceSegmentInfosVersion(forcePrimaryVersion);
                }

                mgr = new SearcherManager(writer, true, true, searcherFactory);
                setCurrentInfos(Collections.< String > emptySet());
                message("init: infos version=" + curInfos.getVersion());

            }
            catch (Exception t)
            {
                Message("init: exception");
                t.printStackTrace(TextWriter);
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
        public bool flushAndRefresh()
        {
            Message("top: now flushAndRefresh");
            Set<String> completedMergeFiles;
            synchronized(finishedMergedFiles) {
                completedMergeFiles = Set.copyOf(finishedMergedFiles);
            }
            mgr.maybeRefreshBlocking();
            boolean result = setCurrentInfos(completedMergeFiles);
            if (result)
            {
                message("top: opened NRT reader version=" + curInfos.getVersion());
                finishedMergedFiles.removeAll(completedMergeFiles);
                message(
                    "flushAndRefresh: version="
                        + curInfos.getVersion()
                        + " completedMergeFiles="
                        + completedMergeFiles
                        + " finishedMergedFiles="
                        + finishedMergedFiles);
            }
            else
            {
                message("top: no changes in flushAndRefresh; still version=" + curInfos.getVersion());
            }
            return result;
        }

        public long getCopyStateVersion()
        {
            return copyState.version;
        }

        public long getLastCommitVersion()
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                Iterable<Map.Entry<String, String>> iter = writer.getLiveCommitData();
                assert iter != null;
                for (Map.Entry<String, String> ent : iter)
                {
                    if (ent.getKey().equals(VERSION_KEY))
                    {
                        return Long.parseLong(ent.getValue());
                    }
                }

                // In ctor we always install an initial version:
                throw new AssertionError("missing VERSION_KEY");
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
            Map<String, String> commitData = new HashMap<>();
            commitData.put(PRIMARY_GEN_KEY, Long.toString(primaryGen));
            // TODO (opto): it's a bit wasteful that we put "last refresh" version here, not the actual
            // version we are committing, because it means
            // on xlog replay we are replaying more ops than necessary.
            commitData.put(VERSION_KEY, Long.toString(copyState.version));
            message("top: commit commitData=" + commitData);
            writer.setLiveCommitData(commitData.entrySet(), false);
            writer.commit();
        }

        /** IncRef the current CopyState and return it */
        /// <exception cref="IOException"/>
        public CopyState getCopyState()
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                ensureOpen(false);
                // message("top: getCopyState replicaID=" + replicaID + " replicaNodeID=" + replicaNodeID + "
                // version=" + curInfos.getVersion() + " infos=" + curInfos.toString());
                assert curInfos == copyState.infos;
                writer.incRefDeleter(copyState.infos);
                int count = copyingCount.incrementAndGet();
                assert count > 0;
                return copyState;
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }


        }

        /** Called once replica is done (or failed) copying an NRT point */
        /// <exception cref="IOException"/>
        public void releaseCopyState(CopyState copyState)
        {
            // message("top: releaseCopyState version=" + copyState.version);
            assert copyState.infos != null;
            writer.decRefDeleter(copyState.infos);
            int count = copyingCount.decrementAndGet();
            assert count >= 0;
        }

        public override bool IsClosed()
        {
            return isClosed(false);
        }

        bool isClosed(bool allowClosing)
        {
            return "closed".Equals(state) || (allowClosing == false && "closing".Equals(state));
        }

        private void ensureOpen(bool allowClosing)
        {
            if (isClosed(allowClosing))
            {
                throw new AlreadyClosedException(state);
            }
        }

        /// <remarks>
        /// Steals incoming infos refCount; returns true if there were changes.
        /// </remarks>
        /// <exception cref="IOException"/>
        private synchronized bool setCurrentInfos(Set<String> completedMergeFiles)
        {

            IndexSearcher searcher = null;
            SegmentInfos infos;
            try
            {
                searcher = mgr.acquire();
                infos = ((StandardDirectoryReader)searcher.getIndexReader()).getSegmentInfos();
            }
            finally
            {
                if (searcher != null)
                {
                    mgr.release(searcher);
                }
            }
            if (curInfos != null && infos.getVersion() == curInfos.getVersion())
            {
                // no change
                message(
                    "top: skip switch to infos: version="
                        + infos.getVersion()
                        + " is unchanged: "
                        + infos.toString());
                return false;
            }

            SegmentInfos oldInfos = curInfos;
            writer.incRefDeleter(infos);
            curInfos = infos;
            if (oldInfos != null)
            {
                writer.decRefDeleter(oldInfos);
            }

            message("top: switch to infos=" + infos.toString() + " version=" + infos.getVersion());

            // Serialize the SegmentInfos.
            ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();
            try (ByteBuffersIndexOutput tmpIndexOutput =
                new ByteBuffersIndexOutput(buffer, "temporary", "temporary")) {
                infos.write(tmpIndexOutput);
            }
            byte[] infosBytes = buffer.toArrayCopy();

            Map<String, FileMetaData> filesMetaData = new HashMap<String, FileMetaData>();
            for (SegmentCommitInfo info : infos)
            {
                for (String fileName : info.files())
                {
                    FileMetaData metaData = readLocalFileMetaData(fileName);
                    // NOTE: we hold a refCount on this infos, so this file better exist:
                    assert metaData != null : "file \"" + fileName + "\" is missing metadata";
                    assert filesMetaData.containsKey(fileName) == false;
                    filesMetaData.put(fileName, metaData);
                }
            }

            lastFileMetaData = Collections.unmodifiableMap(filesMetaData);

            message(
                "top: set copyState primaryGen="
                    + primaryGen
                    + " version="
                    + infos.getVersion()
                    + " files="
                    + filesMetaData.keySet());
            copyState =
                new CopyState(
                    lastFileMetaData,
                    infos.getVersion(),
                    infos.getGeneration(),
                    infosBytes,
                    completedMergeFiles,
                    primaryGen,
                    curInfos);
            return true;
            }

        /// <exception cref="IOException"/>
        private void waitForAllRemotesToClose()
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
                        int count = copyingCount.get();
                        if (count == 0)
                        {
                            return;
                        }
                        Message("pendingCopies: " + count);

                        try
                        {
                            wait(10);
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
                waitForAllRemotesToClose();
                if (curInfos != null)
                {
                    writer.decRefDeleter(curInfos);
                    curInfos = null;
                }
            }

            mgr.close();

            writer.rollback();
            dir.close();

            state = "closed";
        }

        /** Called when a merge has finished, but before IW switches to the merged segment */
        /// <exception cref="IOException"/>
        protected abstract void preCopyMergedSegmentFiles(
            SegmentCommitInfo info, Map<String, FileMetaData> files)
    }
}
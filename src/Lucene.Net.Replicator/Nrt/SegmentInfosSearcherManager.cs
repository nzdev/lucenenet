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
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Support.Threading;

namespace Lucene.Net.Replicator.Nrt
{



    /**
     * A SearcherManager that refreshes via an externally provided (NRT) SegmentInfos, either from
     * {@link IndexWriter} or via nrt replication to another index.
     *
     * @lucene.experimental
     */
    class SegmentInfosSearcherManager : ReferenceManager<IndexSearcher>
    {
        private volatile SegmentInfos currentInfos;
        private readonly Directory dir;
        private readonly Node node;
        private readonly AtomicInt32 openReaderCount = new AtomicInt32();
        private readonly SearcherFactory searcherFactory;

        /// <exception cref="IOException"/>
        public SegmentInfosSearcherManager(Directory dir, Node node, SegmentInfos infosIn, SearcherFactory searcherFactory)
        {
            this.dir = dir;
            this.node = node;
            if (searcherFactory == null)
            {
                searcherFactory = new SearcherFactory();
            }
            this.searcherFactory = searcherFactory;
            currentInfos = infosIn;
            node.Message("SegmentInfosSearcherManager.init: use incoming infos=" + infosIn.ToString());
            Current =
                SearcherManager.GetSearcher(
                    searcherFactory, StandardDirectoryReader.Open(dir, currentInfos, null, null), null);
            addReaderClosedListener(Current.GetIndexReader());
        }

        protected override int GetRefCount(IndexSearcher s)
        {
            return s.GetIndexReader().getRefCount();
        }

        protected override bool TryIncRef(IndexSearcher s)
        {
            return s.GetIndexReader().tryIncRef();
        }
        /// <exception cref="IOException"/>
        protected override void DecRef(IndexSearcher s)
        {
            s.GetIndexReader().decRef();
        }

        public SegmentInfos GetCurrentInfos()
        {
            return currentInfos;
        }

        /// <summary>
        /// Switch to new segments, refreshing if necessary. Note that it's the caller job to ensure
        /// there's a held refCount for the incoming infos, so all files exist.
        /// </summary>
        /// <exception cref="IOException"/>
        public void SetCurrentInfos(SegmentInfos infos)
        {
            if (currentInfos != null)
            {
                // So that if we commit, we will go to the next
                // (unwritten so far) generation:
                infos.UpdateGeneration(currentInfos);
                node.Message("mgr.setCurrentInfos: carry over infos gen=" + infos.GetSegmentsFileName());
            }
            currentInfos = infos;
            MaybeRefresh();
        }

        /// <exception cref="IOException"/>
        protected override IndexSearcher RefreshIfNeeded(IndexSearcher old)
        {
            List<LeafReader> subs;
            if (old == null)
            {
                subs = null;
            }
            else
            {
                subs = new ArrayList<>();
                for (LeafReaderContext ctx : old.GetIndexReader().leaves())
                {
                    subs.add(ctx.reader());
                }
            }

            // Open a new reader, sharing any common segment readers with the old one:
            DirectoryReader r = StandardDirectoryReader.Open(dir, currentInfos, subs, null);
            addReaderClosedListener(r);
            node.Message("refreshed to version=" + currentInfos.GetVersion() + " r=" + r);
            return SearcherManager.GetSearcher(searcherFactory, r, old.GetIndexReader());
        }

        private void addReaderClosedListener(IndexReader r)
        {
            IndexReader.CacheHelper cacheHelper = r.GetReaderCacheHelper();
            if (cacheHelper == null)
            {
                throw new IllegalStateException("StandardDirectoryReader must support caching");
            }
            openReaderCount.IncrementAndGet();
            cacheHelper.addClosedListener(
                new IndexReader.ClosedListener()
                {
                      public override void onClose(IndexReader.CacheKey cacheKey)
        {
            onReaderClosed();
        }
    });
  }

/**
 * Tracks how many readers are still open, so that when we are closed, we can additionally wait
 * until all in-flight searchers are closed.
 */
void onReaderClosed()
{
    UninterruptableMonitor.Enter(this);
    try
    {
        if (openReaderCount.decrementAndGet() == 0)
        {
            notifyAll();
        }
    }
    }
    finally
{
    UninterruptableMonitor.Exit(this);
}
    
}
}
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
using System;
using System.Collections.Generic;


/// <summary>
/// A SearcherManager that refreshes via an externally provided (NRT) SegmentInfos, either from {@link IndexWriter} or via
/// nrt replication to another index.
/// </summary>
/// <remarks>
///  @lucene.experimental
/// </remarks>
class SegmentInfosSearcherManager : ReferenceManager<IndexSearcher>
{
    private volatile SegmentInfos currentInfos;
    private readonly Directory dir;
    private readonly Node node;
    private readonly AtomicInt32 openReaderCount = new AtomicInt32();
    private readonly SearcherFactory searcherFactory;

    /// <exception cref="System.IO.IOException"/>
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
        Current = SearcherManager.GetSearcher(searcherFactory, StandardDirectoryReader.Open(dir, currentInfos, null), null);
        AddReaderClosedListener(Current.IndexReader);
    }

    protected override int GetRefCount(IndexSearcher s)
    {
        return s.IndexReader.RefCount;
    }

    protected override bool TryIncRef(IndexSearcher s)
    {
        return s.IndexReader.TryIncRef();
    }

    /// <exception cref="IOException"/>
    protected override void DecRef(IndexSearcher s)
    {
        s.IndexReader.DecRef();
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
            subs = new List<LeafReader>();
            foreach (LeafReaderContext ctx in old.IndexReader.Leaves)
            {
                subs.Add(ctx.reader());
            }
        }

        // Open a new reader, sharing any common segment readers with the old one:
        DirectoryReader r = StandardDirectoryReader.Open(dir, currentInfos, subs);
        AddReaderClosedListener(r);
        node.Message("refreshed to version=" + currentInfos.Version + " r=" + r);
        return SearcherManager.GetSearcher(searcherFactory, r, (DirectoryReader)old.IndexReader);
    }

    private class ReaderClosedListenerAction : IndexReader.IReaderClosedListener
    {
        private readonly Action<IndexReader> action;

        public ReaderClosedListenerAction(Action<IndexReader> action)
        {
            this.action = action;
        }
        public void OnClose(IndexReader reader)
        {
            action(reader);
        }
    }

    private void AddReaderClosedListener(IndexReader r)
    {
        openReaderCount.IncrementAndGet();
        r.AddReaderClosedListener(new ReaderClosedListenerAction((reader) => OnReaderClosed()));
    }

    /// <summary>
    /// Tracks how many readers are still open, so that when we are closed,
    /// we can additionally wait until all in-flight searchers are
    /// closed.
    /// </summary>
    void OnReaderClosed()
    {
        UninterruptableMonitor.Enter(this);
        try
        {
            if (openReaderCount.DecrementAndGet() == 0)
            {
                UninterruptableMonitor.PulseAll(this);
            }
        }
        finally
        {
            UninterruptableMonitor.Exit(this);
        }
    }
}

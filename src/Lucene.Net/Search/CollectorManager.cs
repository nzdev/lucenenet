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
using System.Collections.Generic;
using System.IO;

namespace Lucene.Net.Search
{
    /// <summary>
    /// A manager of collectors. This class is useful to parallelize execution of search requests and has
    /// two main methods:
    /// <ul>
    ///   <li>{@link #newCollector()} which must return a NEW collector which will be used to collect a
    /// certain set of leaves.
    ///   <li>{@link #reduce(Collection)} which will be used to reduce the results of individual
    /// collections into a meaningful result.This method is only called after all leaves have been
    /// fully collected.
    /// </ul>
    /// @see IndexSearcher#search(Query, CollectorManager)
    /// </summary>
    /// <remarks>
    /// @lucene.experimental
    /// </remarks>
    public interface CollectorManager<C, T> where C : ICollector
    {

        /// <summary>
        ///  Return a new {@link Collector}. This must return a different instance on each call.
        /// </summary>
        /// <exception cref="IOException"/>
        C NewCollector();

        /// <summary>
        /// Reduce the results of individual collectors into a meaningful result. For instance a {@link
        /// TopDocsCollector} would compute the {@link TopDocsCollector#topDocs() top docs} of each
        /// collector and then merge them using {@link TopDocs#merge(int, TopDocs[])}. This method must be
        /// called after collection is finished on all provided collectors.
        /// </summary>
        /// <exception cref="IOException"/>
        T Reduce(ICollection<C> collectors);
    }
}
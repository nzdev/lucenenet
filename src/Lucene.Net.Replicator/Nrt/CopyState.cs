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

using Lucene.Net.Diagnostics;
using Lucene.Net.Index;
using Lucene.Net.Support;
using System;
using System.Collections.Generic;

namespace Lucene.Net.Replicator.Nrt {

    /// <summary>
    /// Holds incRef'd file level details for one point-in-time segment infos on the primary node.
    /// </summary>
    /// <remarks>
    /// @lucene.experimental
    /// </remarks>
    public class CopyState
    {

        public readonly IDictionary<string, FileMetaData> files;
        public readonly long version;
        public readonly long gen;
        public readonly byte[] infosBytes;
        public readonly ISet<string> completedMergeFiles;
        public readonly long primaryGen;

        // only non-null on the primary node
        public readonly SegmentInfos infos;

        public CopyState(IDictionary<string, FileMetaData> files, long version, long gen, byte[] infosBytes,
                         ISet<string> completedMergeFiles, long primaryGen, SegmentInfos infos)
        {
            if (Debugging.AssertsEnabled)
            {
                Debugging.Assert(completedMergeFiles != null);
            }
            this.files = Collections.UnmodifiableMap(files);
            this.version = version;
            this.gen = gen;
            this.infosBytes = infosBytes;
            this.completedMergeFiles = Collections.UnmodifiableSet(completedMergeFiles);
            this.primaryGen = primaryGen;
            this.infos = infos;
        }

        public override string ToString()
        {
            return nameof(CopyState) + "(version=" + version + ")";
        }
    }
}
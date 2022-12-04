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

//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Locale;
//import java.util.Map;
//import java.util.concurrent.TimeUnit;
//import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
//import org.apache.lucene.index.LeafReader;
//import org.apache.lucene.index.SegmentCommitInfo;
//import org.apache.lucene.index.SegmentReader;
using J2N;
using J2N.Collections.Generic;
using Lucene.Net.Index;
using System.Collections;

namespace Lucene.Net.Replicator.Nrt
{


    /**
     * A merged segment warmer that pre-copies the merged segment out to replicas before primary cuts
     * over to the merged segment. This ensures that NRT reopen time on replicas is only in proportion
     * to flushed segment sizes, not merged segments.
     */


    // TODO: or ... replica node can do merging locally?  tricky to keep things in sync, when one node
    // merges more slowly than others...

    class PreCopyMergedSegmentWarmer : IndexReaderWarmer
    {

        private readonly PrimaryNode primary;

        public PreCopyMergedSegmentWarmer(PrimaryNode primary)
        {
            this.primary = primary;
        }

        /// <exception cref="IOException"/>
        public override void Warm(LeafReader reader)
        {
            long startNS = Time.NanoTime();
            readonly SegmentCommitInfo info = ((SegmentReader)reader).getSegmentInfo();
            // System.out.println("TEST: warm merged segment files " + info);
            IDictionary<string, FileMetaData> filesMetaData = new Dictionary<string, FileMetaData>();
            for (String fileName : info.files())
            {
                FileMetaData metaData = primary.readLocalFileMetaData(fileName);
                assert metaData != null;
                assert filesMetaData.containsKey(fileName) == false;
                filesMetaData.put(fileName, metaData);
            }

            primary.PreCopyMergedSegmentFiles(info, filesMetaData);
            primary.Message(
                string.Format(
                    Locale.ROOT,
                    "top: done warm merge " + info + ": took %.3f sec, %.1f MB",
                    (Time.NanoTime() - startNS) / (double)TimeUnit.SECONDS.toNanos(1),
                    info.sizeInBytes() / 1024. / 1024.));
            primary.finishedMergedFiles.addAll(filesMetaData.keySet());
        }
    }
}
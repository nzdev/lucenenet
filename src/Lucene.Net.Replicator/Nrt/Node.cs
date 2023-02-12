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


using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using System;
using Lucene.Net.Codecs;
using J2N;
using System.Collections.Generic;
using Directory = Lucene.Net.Store.Directory;
using System.Threading;
using System.IO;

namespace Lucene.Net.Replicator.Nrt
{
    /// <summary>
    /// Common base class for {@link PrimaryNode} and {@link ReplicaNode}.
    /// </summary>
    /// <remarks>
    /// @lucene.experimental
    /// </remarks>
    public abstract class Node : IDisposable
    {

        public static bool VERBOSE_FILES = true;
        public static bool VERBOSE_CONNECTIONS = false;

        // Keys we store into IndexWriter's commit user data:

        /** Key to store the primary gen in the commit data, which increments every time we promote a new primary, so replicas can detect when the
         *  primary they were talking to is changed */
        public static String PRIMARY_GEN_KEY = "__primaryGen";

        /** Key to store the version in the commit data, which increments every time we open a new NRT reader */
        public static String VERSION_KEY = "__version";

        /** Compact ordinal for this node */
        protected readonly int id;

        protected readonly Directory dir;

        protected readonly SearcherFactory searcherFactory;

        // Tracks NRT readers, opened from IW (primary) or opened from replicated SegmentInfos pulled across the wire (replica):
        protected ReferenceManager<IndexSearcher> mgr;

        /** Startup time of original test, carefully propogated to all nodes to produce consistent "seconds since start time" in messages */
        public static long globalStartNS;

        /** When this node was started */
        public static readonly long localStartNS = Time.NanoTime();

        /** For debug logging */
        protected readonly TextWriter printStream;

        // public static final long globalStartNS;

        // For debugging:
        protected volatile string state = "idle";

        /** File metadata for last sync that succeeded; we use this as a cache */
        protected volatile IDictionary<String, FileMetaData> lastFileMetaData;

        public Node(int id, Directory dir, SearcherFactory searcherFactory, TextWriter printStream)
        {
            this.id = id;
            this.dir = dir;
            this.searcherFactory = searcherFactory;
            this.printStream = printStream;
        }

        public override string ToString()
        {
            return nameof(Node) + "(id=" + id + ")";
        }

        /// <exception cref="IOException"/>
        public abstract void Commit();

        public static void NodeMessage(TextWriter printStream, String message)
        {
            if (printStream != null)
            {
                long now = Time.NanoTime();
                printStream.WriteLine(
                    String.Format(
                        "%5.3fs %5.1fs:           [%11s] %s",
                        (now - globalStartNS) / (double)Extensions.TimeUnitSecondsToNanos(1),
                        (now - localStartNS) / (double)Extensions.TimeUnitSecondsToNanos(1),
                        Thread.CurrentThread.Name,
                        message));
            }
        }

        public static void NodeMessage(TextWriter printStream, int id, string message)
        {
            if (printStream != null)
            {
                long now = Time.NanoTime();
                printStream.WriteLine(
                    string.Format(
                        "%5.3fs %5.1fs:         N%d [%11s] %s",
                        (now - globalStartNS) / (double)Extensions.TimeUnitSecondsToNanos(1),
                        (now - localStartNS) / (double)Extensions.TimeUnitSecondsToNanos(1),
                        id,
                        Thread.CurrentThread.Name,
                        message));
            }
        }

        internal void Message(string message)
        {
            if (printStream != null)
            {
                long now = Time.NanoTime();
                printStream.WriteLine(
                    String.Format(
                        "%5.3fs %5.1fs: %7s %2s [%11s] %s",
                        (now - globalStartNS) / (double)Extensions.TimeUnitSecondsToNanos(1),
                        (now - localStartNS) / (double)Extensions.TimeUnitSecondsToNanos(1),
                        state,
                        Name(),
                         Thread.CurrentThread.Name,
                        message));
            }
        }

        public String Name()
        {
            char mode = this is PrimaryNode ? 'P' : 'R';
            return mode + id.ToString();
        }

        public abstract bool IsClosed();

        /// <exception cref="System.IO.IOException"/>
        public long GetCurrentSearchingVersion()
        {
            IndexSearcher searcher = mgr.Acquire();
            try
            {
                return ((DirectoryReader)searcher.GetIndexReader()).Version;
            }
            finally
            {
                mgr.Release(searcher);
            }
        }

        public static string BytesToString(long bytes)
        {
            if (bytes < 1024)
            {
                return bytes + " b";
            }
            else if (bytes < 1024 * 1024)
            {
                return string.Format("%.1f KB", bytes / 1024.0);
            }
            else if (bytes < 1024 * 1024 * 1024)
            {
                return string.Format("%.1f MB", bytes / 1024.0 / 1024.0);
            }
            else
            {
                return string.Format("%.1f GB", bytes / 1024.0 / 1024.0 / 1024.0);
            }
        }

        /** Opens the specified file, reads its identifying information, including file length, full index header (includes the unique segment
         *  ID) and the full footer (includes checksum), and returns the resulting {@link FileMetaData}.
         *
         *  <p>This returns null, logging a message, if there are any problems (the file does not exist, is corrupt, truncated, etc.).</p> */
        /// <exception cref="System.IO.IOException"/>
        public FileMetaData ReadLocalFileMetaData(String fileName)
        {

            IDictionary<String, FileMetaData> cache = lastFileMetaData;
            FileMetaData result;
            if (cache != null)
            {
                // We may already have this file cached from the last NRT point:
                result = cache[fileName];
            }
            else
            {
                result = null;
            }

            if (result == null)
            {
                // Pull from the filesystem
                long checksum;
                long length;
                byte[] header;
                byte[] footer;
                try
                {
                    IndexInput @in = dir.OpenInput(fileName, IOContext.DEFAULT);
                    try
                    {
                        length = @in.Length;
                        header = CodecUtil.ReadIndexHeader(@in);
                        footer = CodecUtil.ReadFooter(@in);
                        checksum = CodecUtil.RetrieveChecksum(@in);
                    }
                    catch (Exception cie) when (cie is EOFException || cie is CorruptIndexException)
                    {
                        // File exists but is busted: we must copy it.  This happens when node had crashed, corrupting an un-fsync'd file.  On init we try
                        // to delete such unreferenced files, but virus checker can block that, leaving this bad file.
                        if (VERBOSE_FILES)
                        {
                            Message("file " + fileName + ": will copy [existing file is corrupt]");
                        }
                        return null;
                    }
                    if (VERBOSE_FILES)
                    {
                        Message("file " + fileName + " has length=" + BytesToString(length));
                    }
                }
                catch (Exception cie) when (cie is FileNotFoundException/* || cie is NoSuchFileException*/)
                {
                    if (VERBOSE_FILES)
                    {
                        Message("file " + fileName + ": will copy [file does not exist]");
                    }
                    return null;
                }

                // NOTE: checksum is redundant w/ footer, but we break it out separately because when the bits cross the wire we need direct access to
                // checksum when copying to catch bit flips:
                result = new FileMetaData(header, footer, length, checksum);
            }

            return result;
        }
    }
}
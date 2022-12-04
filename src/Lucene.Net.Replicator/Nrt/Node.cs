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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Directory = Lucene.Net.Store.Directory;
using System.Threading;

//import java.io.EOFException;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.TextWriter;
//import java.nio.file.NoSuchFileException;
//import java.util.Locale;
//import java.util.Map;
//import java.util.concurrent.TimeUnit;


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

        /**
         * Key to store the primary gen in the commit data, which increments every time we promote a new
         * primary, so replicas can detect when the primary they were talking to is changed
         */
        public static String PRIMARY_GEN_KEY = "__primaryGen";

        /**
         * Key to store the version in the commit data, which increments every time we open a new NRT
         * reader
         */
        public static String VERSION_KEY = "__version";

        /** Compact ordinal for this node */
        protected readonly int id;

        protected readonly Directory dir;

        protected readonly SearcherFactory searcherFactory;

        // Tracks NRT readers, opened from IW (primary) or opened from replicated SegmentInfos pulled
        // across the wire (replica):
        protected ReferenceManager<IndexSearcher> mgr;

        /**
         * Startup time of original test, carefully propogated to all nodes to produce consistent "seconds
         * since start time" in messages
         */
        public static long globalStartNS;

        /** When this node was started */
        public static readonly long localStartNS = Time.NanoTime();

        /** For debug logging */
        protected readonly TextWriter TextWriter;

        // public static final long globalStartNS;

        // For debugging:
        protected volatile String state = "idle";

        /** File metadata for last sync that succeeded; we use this as a cache */
        protected volatile IDictionary<String, FileMetaData> lastFileMetaData;

        public Node(int id, Directory dir, SearcherFactory searcherFactory, TextWriter TextWriter)
        {
            this.id = id;
            this.dir = dir;
            this.searcherFactory = searcherFactory;
            this.TextWriter = TextWriter;
        }

        /** Returns the {@link ReferenceManager} to use for acquiring and releasing searchers */
        public ReferenceManager<IndexSearcher> GetSearcherManager()
        {
            return mgr;
        }

        /** Returns the {@link Directory} this node is writing to */
        public Directory GetDirectory()
        {
            return dir;
        }

        public override string ToString()
        {
            return nameof(Node) + "(id=" + id + ")";
        }

        /// <exception cref="IOException"/>
        public abstract void Commit();

        public static void NodeMessage(TextWriter TextWriter, String message)
        {
            if (TextWriter != null)
            {
                long now = Time.NanoTime();
                TextWriter.WriteLine(
                    String.Format(
                        Locale.ROOT,
                        "%5.3fs %5.1fs:           [%11s] %s",
                        (now - globalStartNS) / (double)TimeUnit.SECONDS.toNanos(1),
                        (now - localStartNS) / (double)TimeUnit.SECONDS.toNanos(1),
                        Thread.CurrentThread.Name,
                        message));
            }
        }

        public static void NodeMessage(TextWriter TextWriter, int id, String message)
        {
            if (TextWriter != null)
            {
                long now = Time.NanoTime();
                TextWriter.WriteLine(
                    String.Format(
                        Locale.ROOT,
                        "%5.3fs %5.1fs:         N%d [%11s] %s",
                        (now - globalStartNS) / (double)TimeUnit.SECONDS.toNanos(1),
                        (now - localStartNS) / (double)TimeUnit.SECONDS.toNanos(1),
                        id,
                        Thread.CurrentThread.Name,
                        message));
            }
        }

        public void Message
            (String message)
        {
            if (TextWriter != null)
            {
                long now = Time.NanoTime();
                TextWriter.WriteLine(
                    String.Format(
                        Locale.ROOT,
                        "%5.3fs %5.1fs: %7s %2s [%11s] %s",
                        (now - globalStartNS) / (double)TimeUnit.SECONDS.toNanos(1),
                        (now - localStartNS) / (double)TimeUnit.SECONDS.toNanos(1),
                        state,
                        Name(),
                         Thread.CurrentThread.Name,
                        message));
            }
        }

        public String Name()
        {
            char mode = this instanceof PrimaryNode ? 'P' : 'R';
            return mode + Integer.toString(id);
        }

        public abstract bool IsClosed();

        /// <exception cref="IOException"/>
        public long GetCurrentSearchingVersion()
        {
            IndexSearcher searcher = mgr.Acquire();
            try
            {
                return ((DirectoryReader)searcher.GetIndexReader()).getVersion();
            }
            finally
            {
                mgr.Release(searcher);
            }
        }

        public static String BytesToString(long bytes)
        {
            if (bytes < 1024)
            {
                return bytes + " b";
            }
            else if (bytes < 1024 * 1024)
            {
                return String.Format(Locale.ROOT, "%.1f KB", bytes / 1024.);
            }
            else if (bytes < 1024 * 1024 * 1024)
            {
                return String.Format(Locale.ROOT, "%.1f MB", bytes / 1024. / 1024.);
            }
            else
            {
                return String.Format(Locale.ROOT, "%.1f GB", bytes / 1024. / 1024. / 1024.);
            }
        }

        /**
         * Opens the specified file, reads its identifying information, including file length, full index
         * header (includes the unique segment ID) and the full footer (includes checksum), and returns
         * the resulting {@link FileMetaData}.
         *
         * <p>This returns null, logging a message, if there are any problems (the file does not exist, is
         * corrupt, truncated, etc.).
         */

        /// <exception cref="IOException"/>
        public FileMetaData ReadLocalFileMetaData(String fileName)
        {

            IDictionary<String, FileMetaData> cache = lastFileMetaData;
            FileMetaData result;
            if (cache != null)
            {
                // We may already have this file cached from the last NRT point:
                result = cache.[fileName];
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
                using (IndexInput @in = dir.OpenInput(fileName, IOContext.DEFAULT))
                {
                    try
                    {
                        length = @in.Length;
                        header = CodecUtil.ReadIndexHeader(@in);
                        footer = CodecUtil.ReadFooter(@in);
                        checksum = CodecUtil.RetrieveChecksum(@in);
                    }
                    //@SuppressWarnings("unused")
                    catch (Exception cie) when (cie is EOFException || cie is CorruptIndexException)
                    {
                        // File exists but is busted: we must copy it.  This happens when node had crashed,
                        // corrupting an un-fsync'd file.  On init we try
                        // to delete such unreferenced files, but virus checker can block that, leaving this bad
                        // file.
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
                //@SuppressWarnings("unused")
                    catch (Exception e) when (e is FileNotFoundException || e is NoSuchFileException)
                {
                    if (VERBOSE_FILES)
                    {
                        Message("file " + fileName + ": will copy [file does not exist]");
                    }
                    return null;
                }

                // NOTE: checksum is redundant w/ footer, but we break it out separately because when the bits
                // cross the wire we need direct access to
                // checksum when copying to catch bit flips:
                result = new FileMetaData(header, footer, length, checksum);
            }

            return result;
        }
    }

}
    }

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance withnamespace Lucene.Net.Replicator.Nrt
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
using JCG = J2N.Collections.Generic;
using J2N.Threading.Atomic;
using Lucene.Net.Replicator.Nrt;
using Lucene.Net.Support;
using Lucene.Net.Support.Threading;
using Lucene.Net.Util;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using Lucene.Net.Diagnostics;
using Lucene.Net.Index;

//import java.io.IOException;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Locale;
//import java.util.Set;
//import org.apache.lucene.index.CorruptIndexException;
//import org.apache.lucene.util.IOUtils;

namespace Lucene.Net.Replicator.Nrt
{
    /**
     * Handles copying one set of files, e.g. all files for a new NRT point, or files for pre-copying a
     * merged segment. This notifies the caller via OnceDone when the job finishes or failed.
     *
     * @lucene.experimental
     */
    public abstract class CopyJob : IComparable<CopyJob>
    {
        private static readonly AtomicInt64 counter = new AtomicInt64();
        protected readonly ReplicaNode dest;

        protected readonly IDictionary<String, FileMetaData> files;

        public readonly long ord = counter.IncrementAndGet();

        /// <remarks>
        /// True for an NRT sync, false for pre-copying a newly merged segment
        /// </remarks>
        public readonly bool highPriority;

        public readonly OnceDone onceDone;

        public readonly long startNS = Time.NanoTime();
        public readonly string reason;

        protected readonly IList<KeyValuePair<String, FileMetaData>> toCopy;

        protected long totBytes;

        protected long totBytesCopied;

        /// <summary>
        /// The file we are currently copying:
        /// </summary>
        protected CopyOneFile current;

        /// <remarks>
        /// Set when we are cancelled
        /// </remarks>
        protected volatile Exception exc;
        protected volatile string cancelReason;

        /// <remarks>
        /// toString may concurrently access this:
        /// </remarks>
        protected readonly IDictionary<string, string> copiedFiles = new ConcurrentDictionary<string, string>();

        /// <exception cref="IOException"/>
        protected CopyJob(
        string reason,
        IDictionary<String, FileMetaData> files,
        ReplicaNode dest,
        bool highPriority,
        OnceDone onceDone)
        {
            this.reason = reason;
            this.files = files;
            this.dest = dest;
            this.highPriority = highPriority;
            this.onceDone = onceDone;

            // Exceptions in here are bad:
            try
            {
                this.toCopy = dest.GetFilesToCopy(this.files);
            }
            catch (Exception t)
            {
                Cancel("exc during init", t);
                throw new CorruptIndexException("exception while checking local files", "n/a", t);
            }
        }

        /// <summary>
        /// Callback invoked by CopyJob once all files have (finally) finished copying
        /// </summary>
        public interface OnceDone
        {
            /// <exception cref="IOException"/>
            public void run(CopyJob job);
        }

        /// <summary>
        /// Transfers whatever tmp files were already copied in this previous job and cancels the previous
        /// job
        /// </summary>
        /// <exception cref="IOException"/>
        public void TransferAndCancel(CopyJob prevJob)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                lock (prevJob)
                {
                    dest.Message("CopyJob: now transfer prevJob " + prevJob);
                    try
                    {
                        _transferAndCancel(prevJob);
                    }
                    catch (Exception t)
                    {
                        dest.Message("xfer: exc during transferAndCancel");
                        Cancel("exc during transferAndCancel", t);
                        throw IOUtils.RethrowAlways(t);
                    }
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }

        }
        /// <exception cref="IOException"/>
        private void _transferAndCancel(CopyJob prevJob)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                // Caller must already be sync'd on prevJob:
                if (Debugging.AssertsEnabled)
                {
                    Debugging.Assert(UninterruptableMonitor.IsEntered(prevJob));
                }

                if (prevJob.exc != null)
                {
                    // Already cancelled
                    dest.Message("xfer: prevJob was already cancelled; skip transfer");
                    return;
                }

                // Cancel the previous job
                prevJob.exc = new Exception();

                // Carry over already copied files that we also want to copy
                IEnumerator<KeyValuePair<String, FileMetaData>> it = toCopy.GetEnumerator();
                long bytesAlreadyCopied = 0;

                // Iterate over all files we think we need to copy:
                List<KeyValuePair<string, FileMetaData>> toRemove = new List<KeyValuePair<string, FileMetaData>>();

                while (it.MoveNext())
                {
                    KeyValuePair<String, FileMetaData> ent = it.Current;
                    String fileName = ent.Key;
                    String prevTmpFileName = prevJob.copiedFiles[fileName];
                    if (prevTmpFileName != null)
                    {
                        // This fileName is common to both jobs, and the old job already finished copying it (to a
                        // temp file), so we keep it:
                        long fileLength = ent.Value.length;
                        bytesAlreadyCopied += fileLength;
                        dest.Message(
                            "xfer: carry over already-copied file "
                                + fileName
                                + " ("
                                + prevTmpFileName
                                + ", "
                                + fileLength
                                + " bytes)");
                        copiedFiles.Put(fileName, prevTmpFileName);

                        // So we don't try to delete it, below:
                        prevJob.copiedFiles.Remove(fileName);

                        // So it's not in our copy list anymore:
                        it.Remove();
                    }
                    else if (prevJob.current != null && prevJob.current.name.Equals(fileName))
                    {
                        // This fileName is common to both jobs, and it's the file that the previous job was in the
                        // process of copying.  In this case
                        // we continue copying it from the prevoius job.  This is important for cases where we are
                        // copying over a large file
                        // because otherwise we could keep failing the NRT copy and restarting this file from the
                        // beginning and never catch up:
                        dest.Message(
                            "xfer: carry over in-progress file "
                                + fileName
                                + " ("
                                + prevJob.current.tmpName
                                + ") bytesCopied="
                                + prevJob.current.GetBytesCopied()
                                + " of "
                                + prevJob.current.bytesToCopy);
                        bytesAlreadyCopied += prevJob.current.GetBytesCopied();

                        if (Debugging.AssertsEnabled)
                        {
                            Debugging.Assert(current == null);
                        }

                        // must set current first, before writing/read to c.in/out in case that hits an exception,
                        // so that we then close the temp
                        // IndexOutput when cancelling ourselves:
                        current = NewCopyOneFile(prevJob.current);

                        // Tell our new (primary) connection we'd like to copy this file first, but resuming from
                        // how many bytes we already copied last time:
                        // We do this even if bytesToCopy == bytesCopied, because we still need to readLong() the
                        // checksum from the primary connection:
                        if (Debugging.AssertsEnabled)
                        {
                            Debugging.Assert(prevJob.current.GetBytesCopied() <= prevJob.current.bytesToCopy);
                        }

                        prevJob.current = null;

                        totBytes += current.metaData.length;

                        // So it's not in our copy list anymore:
                        it.Remove();
                    }
                    else
                    {
                        dest.Message("xfer: file " + fileName + " will be fully copied");
                    }
                }
                dest.Message("xfer: " + bytesAlreadyCopied + " bytes already copied of " + totBytes);

                // Delete all temp files the old job wrote but we don't need:
                dest.Message("xfer: now delete old temp files: " + prevJob.copiedFiles.Values);
                IOUtils.DeleteFilesIgnoringExceptions(dest.dir, prevJob.copiedFiles.Values);

                if (prevJob.current != null)
                {
                    IOUtils.CloseWhileHandlingException(prevJob.current);
                    if (Node.VERBOSE_FILES)
                    {
                        dest.Message("remove partial file " + prevJob.current.tmpName);
                    }
                    dest.deleter.deleteNewFile(prevJob.current.tmpName);
                    prevJob.current = null;
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
            
        }

        protected abstract CopyOneFile NewCopyOneFile(CopyOneFile current);

        /** Begin copying files */
        /// <exception cref="IOException"/>
        public abstract void Start();

        /**
         * Use current thread (blocking) to do all copying and then return once done, or throw exception
         * on failure
         */
        /// <exception cref="IOException"/>
        public abstract void RunBlocking();

        /// <exception cref="IOException"/>
        public void Cancel(String reason, Exception exc)
        {
            if (this.exc != null)
            {
                // Already cancelled
                return;
            }

            dest.Message(
                String.Format(
                    Locale.ROOT,
                "top: cancel after copying %s; exc=%s:\n  files=%s\n  copiedFiles=%s",
                Node.BytesToString(totBytesCopied),
                exc,
                files == null ? "null" : files.Keys,
                copiedFiles.Keys));

            if (exc == null)
            {
                exc = new Exception();
            }

            this.exc = exc;
            this.cancelReason = reason;

            // Delete all temp files we wrote:
            IOUtils.DeleteFilesIgnoringExceptions(dest.dir, copiedFiles.Values);

            if (current != null)
            {
                IOUtils.CloseWhileHandlingException(current);
                if (Node.VERBOSE_FILES)
                {
                    dest.Message("remove partial file " + current.tmpName);
                }
                dest.deleter.deleteNewFile(current.tmpName);
                current = null;
            }
        }

        /** Return true if this job is trying to copy any of the same files as the other job */
        public abstract bool Conflicts(CopyJob other);

        /** Renames all copied (tmp) files to their true file names */
        /// <exception cref="IOException"/>
        public abstract void Finish();

        public abstract bool GetFailed();

        /** Returns only those file names (a subset of {@link #getFileNames}) that need to be copied */
        public abstract ISet<string> GetFileNamesToCopy();

        /** Returns all file names referenced in this copy job */
        public abstract ISet<string> GetFileNames();

        public abstract CopyState GetCopyState();

        public abstract long GetTotalBytesCopied();
    }
}
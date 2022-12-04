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

using J2N;
using Lucene.Net.Codecs;
using Lucene.Net.Store;
using System;
using System.IO;

namespace Lucene.Net.Replicator.Nrt
{


    /** Copies one file from an incoming DataInput to a dest filename in a local Directory */
    public class CopyOneFile : IDisposable
    {
        private readonly DataInput input;
        private readonly IndexOutput output;
        private readonly ReplicaNode dest;
        public readonly String name;
        public readonly String tmpName;
        public readonly FileMetaData metaData;
        public readonly long bytesToCopy;
        private readonly long copyStartNS;
        private readonly byte[] buffer;

        private long bytesCopied;

        /// <exception cref="IOException"/>
        public CopyOneFile(DataInput input, ReplicaNode dest, String name, FileMetaData metaData, byte[] buffer)
        {
            this.input = input;
            this.name = name;
            this.dest = dest;
            this.buffer = buffer;
            // TODO: pass correct IOCtx, e.g. seg total size
            output = dest.CreateTempOutput(name, "copy", IOContext.DEFAULT);
            tmpName = output.GetName();

            // last 8 bytes are checksum, which we write ourselves after copying all bytes and confirming
            // checksum:
            bytesToCopy = metaData.length - Long.BYTES;

            if (Node.VERBOSE_FILES)
            {
                dest.message(
                    "file "
                        + name
                        + ": start copying to tmp file "
                        + tmpName
                        + " length="
                        + (8 + bytesToCopy));
            }

            copyStartNS = Time.NanoTime();
            this.metaData = metaData;
            dest.StartCopyFile(name);
        }

        /** Transfers this file copy to another input, continuing where the first one left off */
        public CopyOneFile(CopyOneFile other, DataInput input)
        {
            this.input = input;
            this.dest = other.dest;
            this.name = other.name;
            this.output = other.output;
            this.tmpName = other.tmpName;
            this.metaData = other.metaData;
            this.bytesCopied = other.bytesCopied;
            this.bytesToCopy = other.bytesToCopy;
            this.copyStartNS = other.copyStartNS;
            this.buffer = other.buffer;
        }
        /// <exception cref="IOException"/>
        public override void Close()
        {
            output.Close();
            dest.FinishCopyFile(name);
        }

        /** Copy another chunk of bytes, returning true once the copy is done */
        /// <exception cref="IOException"/>
        public bool Visit()
        {
            // Copy up to 640 KB per visit:
            for (int i = 0; i < 10; i++)
            {
                long bytesLeft = bytesToCopy - bytesCopied;
                if (bytesLeft == 0)
                {
                    long checksum = output.GetChecksum();
                    if (checksum != metaData.checksum)
                    {
                        // Bits flipped during copy!
                        dest.message(
                            "file "
                                + tmpName
                                + ": checksum mismatch after copy (bits flipped during network copy?) after-copy checksum="
                                + checksum
                                + " vs expected="
                                + metaData.checksum
                                + "; cancel job");
                        throw new IOException("file " + name + ": checksum mismatch after file copy");
                    }

                    // Paranoia: make sure the primary node is not smoking crack, by somehow sending us an
                    // already corrupted file whose checksum (in its
                    // footer) disagrees with reality:
                    long actualChecksumIn = CodecUtil.readBELong(in);
                    if (actualChecksumIn != checksum)
                    {
                        dest.message(
                            "file "
                                + tmpName
                                + ": checksum claimed by primary disagrees with the file's footer: claimed checksum="
                                + checksum
                                + " vs actual="
                                + actualChecksumIn);
                        throw new IOException("file " + name + ": checksum mismatch after file copy");
                    }
                    CodecUtil.writeBELong(out, checksum);
                    bytesCopied += Long.BYTES;
                    close();

                    if (Node.VERBOSE_FILES)
                    {
                        dest.message(
                            String.Format(
                                Locale.ROOT,
                                "file %s: done copying [%s, %.3fms]",
                                name,
                                Node.bytesToString(metaData.length),
                                (Time.NanoTime() - copyStartNS) / (double)TimeUnit.MILLISECONDS.toNanos(1)));
                    }

                    return true;
                }

                int toCopy = (int)Math.Min(bytesLeft, buffer.Length);
                input.ReadBytes(buffer, 0, toCopy);
                output.WriteBytes(buffer, 0, toCopy);

                // TODO: rsync will fsync a range of the file; maybe we should do that here for large files in
                // case we crash/killed
                bytesCopied += toCopy;
            }

            return false;
        }

        public long GetBytesCopied()
        {
            return bytesCopied;
        }
    }
}
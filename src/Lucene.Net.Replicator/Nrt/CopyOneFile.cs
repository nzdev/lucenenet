
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

/** Copies one file from an incoming DataInput to a dest filename in a local Directory */
namespace Lucene.Net.Replicator.Nrt
{
    public class CopyOneFile : IDisposable
    {
        private readonly DataInput @in;
        private readonly IndexOutput @out;
        private readonly ReplicaNode dest;
        public readonly String name;
        public readonly String tmpName;
        public readonly FileMetaData metaData;
        public readonly long bytesToCopy;
        private readonly long copyStartNS;
        private readonly byte[] buffer;

        private long bytesCopied;

        /// <exception cref="IOException"/>
        public CopyOneFile(DataInput @in, ReplicaNode dest, string name, FileMetaData metaData, byte[] buffer)
        {
            this.@in = @in;
            this.name = name;
            this.dest = dest;
            this.buffer = buffer;
            // TODO: pass correct IOCtx, e.g. seg total size
            @out = dest.CreateTempOutput(name, "copy", IOContext.DEFAULT);
            tmpName = @out.GetName();

            // last 8 bytes are checksum:
            bytesToCopy = metaData.length - 8;

            if (Node.VERBOSE_FILES)
            {
                dest.Message("file " + name + ": start copying to tmp file " + tmpName + " length=" + (8 + bytesToCopy));
            }

            copyStartNS = Time.NanoTime();
            this.metaData = metaData;
            dest.StartCopyFile(name);
        }

        /** Transfers this file copy to another input, continuing where the first one left off */
        public CopyOneFile(CopyOneFile other, DataInput @in)
        {
            this.@in = @in;
            this.dest = other.dest;
            this.name = other.name;
            this.@out = other.@out;
            this.tmpName = other.tmpName;
            this.metaData = other.metaData;
            this.bytesCopied = other.bytesCopied;
            this.bytesToCopy = other.bytesToCopy;
            this.copyStartNS = other.copyStartNS;
            this.buffer = other.buffer;
        }

        /// <exception cref="IOException"/>
        public void Dispose()
        {
            @out.Dispose();
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
                    long checksum = @out.Checksum;
                    if (checksum != metaData.checksum)
                    {
                        // Bits flipped during copy!
                        dest.Message("file " + tmpName + ": checksum mismatch after copy (bits flipped during network copy?) after-copy checksum=" + checksum + " vs expected=" + metaData.checksum + "; cancel job");
                        throw new IOException("file " + name + ": checksum mismatch after file copy");
                    }

                    // Paranoia: make sure the primary node is not smoking crack, by somehow sending us an already corrupted file whose checksum (in its
                    // footer) disagrees with reality:
                    long actualChecksumIn = @in.ReadInt64();
                    if (actualChecksumIn != checksum)
                    {
                        dest.Message("file " + tmpName + ": checksum claimed by primary disagrees with the file's footer: claimed checksum=" + checksum + " vs actual=" + actualChecksumIn);
                        throw new IOException("file " + name + ": checksum mismatch after file copy");
                    }
                    @out.WriteInt64(checksum);
                    Close();

                    if (Node.VERBOSE_FILES)
                    {
                        dest.Message(String.Format("file %s: done copying [%s, %.3fms]",
                                                   name,
                                                   Node.BytesToString(metaData.length),
                                                   ((Time.NanoTime() - copyStartNS) / 1000000.0)));
                    }

                    return true;
                }

                int toCopy = (int)Math.Min(bytesLeft, buffer.Length);
                @in.ReadBytes(buffer, 0, toCopy);
                @out.WriteBytes(buffer, 0, toCopy);

                // TODO: rsync will fsync a range of the file; maybe we should do that here for large files in case we crash/killed
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
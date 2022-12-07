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

using J2N.IO;
using J2N.Numerics;
using Lucene.Net.Support;
using Lucene.Net.Util;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

//import java.io.EOFException;
//import java.io.IOException;
//import java.nio.BufferUnderflowException;
//import java.nio.ByteBuffer;
//import java.nio.ByteOrder;
//import java.nio.SingleBuffer;
//import java.nio.Int64Buffer;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Locale;
//import java.util.stream.Collectors;
//import org.apache.lucene.util.Accountable;
//import org.apache.lucene.util.RamUsageEstimator;

namespace Lucene.Net.Store
{

    ///<summary>
    /// A {@link DataInput} implementing {@link RandomAccessInput} and reading data from a list of {@link
    /// ByteBuffer}s.
    /// </summary>
    public sealed class ByteBufferDataInput : DataInput, IAccountable, IRandomAccessInput
    {
        private readonly ByteBuffer[] blocks;
        private readonly SingleBuffer[] floatBuffers;
        private readonly Int64Buffer[] longBuffers;
        private readonly int blockBits;
        private readonly int blockMask;
        private readonly long size;
        private readonly long offset;

        private long pos;

        private const int FLOAT_BYTES = 4;
        private const int SHORT_BYTES = 2;
        private const int INT32_BYTES = 4;
        private const int LONG_BYTES = 8;

        /// <summary>
        /// Read data from a set of contiguous buffers. All data buffers except for the last one must have
        /// an identical remaining number of bytes in the buffer (that is a power of two). The last buffer
        /// can be of an arbitrary remaining length.
        /// </summary>
        public ByteBufferDataInput(IList<ByteBuffer> buffers)
        {
            EnsureAssumptions(buffers);

            this.blocks =
                buffers.Select(x => x.AsReadOnlyBuffer().SetOrder(ByteOrder.LittleEndian)).ToArray();
            // pre-allocate these arrays and create the view buffers lazily
            this.floatBuffers = new SingleBuffer[blocks.Length * FLOAT_BYTES];
            this.longBuffers = new Int64Buffer[blocks.Length * LONG_BYTES];
            if (blocks.Length == 1)
            {
                this.blockBits = 32;
                this.blockMask = ~0;
            }
            else
            {
                int blockBytes = DetermineBlockPage(buffers);
                this.blockBits = BitOperation.TrailingZeroCount(blockBytes);
                this.blockMask = (1 << blockBits) - 1;
            }

            this.size = blocks.Select(x => x.Remaining).Sum();

            // The initial "position" of this stream is shifted by the position of the first block.
            this.offset = blocks[0].Position;
            this.pos = offset;
        }

        public long Size()
        {
            return size;
        }

        public long RamBytesUsed()
        {
            // Return a rough estimation for allocated blocks. Note that we do not make
            // any special distinction for what the type of buffer is (direct vs. heap-based).
            return (long)RamUsageEstimator.NUM_BYTES_OBJECT_REF * blocks.Length
                + blocks.Select(x => x.Capacity).Sum();
        }
        /// <exception cref="EOFException"/>
        public override byte ReadByte()
        {
            try
            {
                ByteBuffer block = blocks[BlockIndex(pos)];
                byte v = block.Get(BlockOffset(pos));
                pos++;
                return v;
            }
            catch (IndexOutOfBoundsException e)
            {
                if (pos >= Size())
                {
                    throw EOFException.Create();
                }
                else
                {
                    throw e; // Something is wrong.
                }
            }
        }

        /// <summary>
        /// Reads exactly {@code len} bytes into the given buffer. The buffer must have enough remaining
        /// limit.
        /// If there are fewer than {@code len} bytes in the input, {@link EOFException} is thrown.
        /// </summary>
        /// <exception cref="EOFException"/>
        public override void ReadBytes(ByteBuffer buffer, int len)
        {
            try
            {
                while (len > 0)
                {
                    ByteBuffer block = blocks[BlockIndex(pos)].Duplicate();
                    int blockOffset = BlockOffset(pos);
                    block.SetPosition(blockOffset);
                    int chunk = Math.Min(len, block.Remaining);
                    if (chunk == 0)
                    {
                        throw EOFException.Create();
                    }

                    // Update pos early on for EOF detection on output buffer, then try to get buffer content.
                    pos += chunk;
                    block.SetLimit(blockOffset + chunk);
                    buffer.Put(block);

                    len -= chunk;
                }
            }
            catch (Exception e) when (e is BufferUnderflowException || e is ArrayIndexOutOfBoundsException)
            {
                if (pos >= Size())
                {
                    throw EOFException.Create();
                }
                else
                {
                    throw e; // Something is wrong.
                }
            }
        }

        /// <exception cref="EOFException"/>
        public override void ReadBytes(byte[] arr, int off, int len)
        {
            try
            {
                while (len > 0)
                {
                    ByteBuffer block = blocks[BlockIndex(pos)].Duplicate();
                    block.SetPosition(BlockOffset(pos));
                    int chunk = Math.Min(len, block.Remaining);
                    if (chunk == 0)
                    {
                        throw EOFException.Create();
                    }

                    // Update pos early on for EOF detection, then try to get buffer content.
                    pos += chunk;
                    block.Get(arr, off, chunk);

                    len -= chunk;
                    off += chunk;
                }
            }
            catch (Exception e) when (e is BufferUnderflowException || e is ArrayIndexOutOfBoundsException)
            {
                if (pos >= Size())
                {
                    throw EOFException.Create();
                }
                else
                {
                    throw e; // Something is wrong.
                }
            }
        }

        /// <exception cref="IOException"/>
        public override short ReadShort()
        {
            int blockOffset = BlockOffset(pos);
            if (blockOffset + SHORT_BYTES <= blockMask)
            {
                short v = blocks[BlockIndex(pos)].GetInt16(blockOffset);
                pos += SHORT_BYTES;
                return v;
            }
            else
            {
                return base.ReadShort();
            }
        }


        /// <exception cref="IOException"/>
        public override int ReadInt()
        {
            int blockOffset = BlockOffset(pos);
            if (blockOffset + INT32_BYTES <= blockMask)
            {
                int v = blocks[BlockIndex(pos)].GetInt32(blockOffset);
                pos += INT32_BYTES;
                return v;
            }
            else
            {
                return base.ReadInt();
            }
        }

        /// <exception cref="IOException"/>
        public long ReadLong()
        {
            int blockOffset = BlockOffset(pos);
            if (blockOffset + LONG_BYTES <= blockMask)
            {
                long v = blocks[BlockIndex(pos)].GetInt64(blockOffset);
                pos += LONG_BYTES;
                return v;
            }
            else
            {
                return base.ReadLong();
            }
        }

        public override byte ReadByte(long pos)
        {
            pos += offset;
            return blocks[BlockIndex(pos)].Get(BlockOffset(pos));
        }

        public override short ReadShort(long pos)
        {
            long absPos = offset + pos;
            int blockOffset = BlockOffset(absPos);
            if (blockOffset + SHORT_BYTES <= blockMask)
            {
                return blocks[BlockIndex(absPos)].GetInt16(blockOffset);
            }
            else
            {
                return (short)((ReadByte(pos) & 0xFF) | (ReadByte(pos + 1) & 0xFF) << 8);
            }
        }

        public override int ReadInt(long pos)
        {
            long absPos = offset + pos;
            int blockOffset = BlockOffset(absPos);
            if (blockOffset + INT32_BYTES <= blockMask)
            {
                return blocks[BlockIndex(absPos)].GetInt32(blockOffset);
            }
            else
            {
                return ((ReadByte(pos) & 0xFF)
                    | (ReadByte(pos + 1) & 0xFF) << 8
                    | (ReadByte(pos + 2) & 0xFF) << 16
                    | (ReadByte(pos + 3) << 24));
            }
        }

        public override long ReadLong(long pos)
        {
            long absPos = offset + pos;
            int blockOffset = BlockOffset(absPos);
            if (blockOffset + LONG_BYTES <= blockMask)
            {
                return blocks[BlockIndex(absPos)].GetInt64(blockOffset);
            }
            else
            {
                byte b1 = ReadByte(pos);
                byte b2 = ReadByte(pos + 1);
                byte b3 = ReadByte(pos + 2);
                byte b4 = ReadByte(pos + 3);
                byte b5 = ReadByte(pos + 4);
                byte b6 = ReadByte(pos + 5);
                byte b7 = ReadByte(pos + 6);
                byte b8 = ReadByte(pos + 7);
                return (b8 & 0xFFL) << 56
                    | (b7 & 0xFFL) << 48
                    | (b6 & 0xFFL) << 40
                    | (b5 & 0xFFL) << 32
                    | (b4 & 0xFFL) << 24
                    | (b3 & 0xFFL) << 16
                    | (b2 & 0xFFL) << 8
                    | (b1 & 0xFFL);
            }
        }
        /// <exception cref="EOFException"/>
        public override void ReadFloats(float[] arr, int off, int len)
        {
            try
            {
                while (len > 0)
                {
                    SingleBuffer floatBuffer = GetFloatBuffer(pos);
                    floatBuffer.SetPosition(BlockOffset(pos) >> 2);
                    int chunk = Math.Min(len, floatBuffer.Remaining);
                    if (chunk == 0)
                    {
                        // read a single float spanning the boundary between two buffers
                        arr[off] = Float.intBitsToFloat(ReadInt(pos - offset));
                        off++;
                        len--;
                        pos += FLOAT_BYTES;
                        continue;
                    }

                    // Update pos early on for EOF detection, then try to get buffer content.
                    pos += chunk << 2;
                    floatBuffer.get(arr, off, chunk);

                    len -= chunk;
                    off += chunk;
                }
            }
            catch (Exception e) when (e is BufferUnderflowException || e is IndexOutOfBoundsException)
            {
                if (pos - offset + FLOAT_BYTES > Size())
                {
                    throw EOFException.Create();
                }
                else
                {
                    throw e; // Something is wrong.
                }
            }
        }

        /// <exception cref="EOFException"/>
        public override void ReadLongs(long[] arr, int off, int len)
        {
            try
            {
                while (len > 0)
                {
                    Int64Buffer longBuffer = GetLongBuffer(pos);
                    longBuffer.SetPosition(BlockOffset(pos) >> 3);
                    int chunk = Math.Min(len, longBuffer.Remaining);
                    if (chunk == 0)
                    {
                        // read a single long spanning the boundary between two buffers
                        arr[off] = ReadLong(pos - offset);
                        off++;
                        len--;
                        pos += LONG_BYTES;
                        continue;
                    }

                    // Update pos early on for EOF detection, then try to get buffer content.
                    pos += chunk << 3;
                    longBuffer.Get(arr, off, chunk);

                    len -= chunk;
                    off += chunk;
                }
            }
            catch (Exception e) when (e is BufferUnderflowException || e is IndexOutOfBoundsException)
            {
                if (pos - offset + LONG_BYTES > Size())
                {
                    throw EOFException.Create();
                }
                else
                {
                    throw e; // Something is wrong.
                }
            }
        }

        private SingleBuffer GetFloatBuffer(long pos)
        {
            // This creates a separate SingleBuffer for each observed combination of ByteBuffer/alignment
            int bufferIndex = BlockIndex(pos);
            int alignment = (int)pos & 0x3;
            int floatBufferIndex = bufferIndex * FLOAT_BYTES + alignment;
            if (floatBuffers[floatBufferIndex] == null)
            {
                ByteBuffer dup = blocks[bufferIndex].Duplicate();
                dup.SetPosition(alignment);
                floatBuffers[floatBufferIndex] = dup.SetOrder(ByteOrder.LittleEndian).AsSingleBuffer();
            }
            return floatBuffers[floatBufferIndex];
        }

        private Int64Buffer GetLongBuffer(long pos)
        {
            // This creates a separate Int64Buffer for each observed combination of ByteBuffer/alignment
            int bufferIndex = BlockIndex(pos);
            int alignment = (int)pos & 0x7;
            int longBufferIndex = bufferIndex * LONG_BYTES + alignment;
            if (longBuffers[longBufferIndex] == null)
            {
                ByteBuffer dup = blocks[bufferIndex].Duplicate();
                dup.SetPosition(alignment);
                longBuffers[longBufferIndex] = dup.SetOrder(ByteOrder.LittleEndian).AsInt64Buffer();
            }
            return longBuffers[longBufferIndex];
        }

        public long Position()
        {
            return pos - offset;
        }

        /// <exception cref="EOFException"/>
        public void Seek(long position)
        {
            this.pos = position + offset;
            if (position > Size())
            {
                this.pos = Size();
                throw EOFException.Create();
            }
        }

        /// <exception cref="IOException"/>
        public override void SkipBytes(long numBytes)
        {
            if (numBytes < 0)
            {
                throw new IllegalArgumentException("numBytes must be >= 0, got " + numBytes);
            }
            long skipTo = Position() + numBytes;
            Seek(skipTo);
        }

        public ByteBuffersDataInput Slice(long offset, long length)
        {
            if (offset < 0 || length < 0 || offset + length > this.size)
            {
                throw new IllegalArgumentException(
                    string.Format(
                        "slice(offset=%s, length=%s) is out of bounds: %s",
                        offset,
                        length,
                        this));
            }

            return new ByteBuffersDataInput(SliceBufferList(Arrays.AsList(this.blocks), offset, length));
        }

        public override string ToString()
        {
            return string.Format(
                "%,d bytes, block size: %,d, blocks: %,d, position: %,d%s",
                Size(),
                BlockSize(),
                blocks.Length,
                Position(),
                offset == 0 ? "" : string.Format(" [offset: %,d]", offset));
        }

        private sealed int BlockIndex(long pos)
        {
            return Convert.ToInt32(pos >> blockBits);
        }

        private sealed int BlockOffset(long pos)
        {
            return (int)pos & blockMask;
        }

        private int BlockSize()
        {
            return 1 << blockBits;
        }

        private static bool IsPowerOfTwo(int v)
        {
            return (v & (v - 1)) == 0;
        }

        private static void EnsureAssumptions(IList<ByteBuffer> buffers)
        {
            if (!buffers.Any())
            {
                throw new IllegalArgumentException("Buffer list must not be empty.");
            }

            if (buffers.Count == 1)
            {
                // Special case of just a single buffer, conditions don't apply.
            }
            else
            {
                int blockPage = DetermineBlockPage(buffers);

                // First buffer decides on block page length.
                if (!IsPowerOfTwo(blockPage))
                {
                    throw new IllegalArgumentException(
                        "The first buffer must have power-of-two position() + remaining(): 0x"
                            + Integer.toHexString(blockPage));
                }

                // Any block from 2..last-1 should have the same page size.
                for (int i = 1, last = buffers.Count - 1; i < last; i++)
                {
                    ByteBuffer buffer = buffers[i];
                    if (buffer.Position != 0)
                    {
                        throw new IllegalArgumentException(
                            "All buffers except for the first one must have position() == 0: " + buffer);
                    }
                    if (i != last && buffer.Remaining != blockPage)
                    {
                        throw new IllegalArgumentException(
                            "Intermediate buffers must share an identical remaining() power-of-two block size: 0x"
                                + Integer.toHexString(blockPage));
                    }
                }
            }
        }

        static int DetermineBlockPage(IList<ByteBuffer> buffers)
        {
            ByteBuffer first = buffers[0];
            int blockPage = Convert.ToInt32((long)first.Position + first.Remaining);
            return blockPage;
        }

        private static IList<ByteBuffer> SliceBufferList(
            IList<ByteBuffer> buffers, long offset, long length)
        {
            EnsureAssumptions(buffers);

            if (buffers.Count == 1)
            {
                ByteBuffer cloned = buffers[0].AsReadOnlyBuffer().SetOrder(ByteOrder.LittleEndian);

                cloned.SetPosition(Convert.ToInt32(cloned.Position + offset));
                cloned.SetLimit(Convert.ToInt32(cloned.Position + length));
                return Arrays.AsList(cloned);
            }
            else
            {
                long absStart = buffers.get(0).position() + offset;
                long absEnd = absStart + length;

                int blockBytes = ByteBuffersDataInput.DetermineBlockPage(buffers);
                int blockBits = Integer.numberOfTrailingZeros(blockBytes);
                long blockMask = (1L << blockBits) - 1;

                int endOffset = Convert.ToInt32(absEnd & blockMask);

                IList<ByteBuffer> cloned =
                    buffers
                    .Skip(Convert.ToInt32(absStart / blockBytes)) //startIndex  check for off by one
                    .Take(Convert.ToInt32(absEnd / blockBytes + (endOffset == 0 ? 0 : 1))) // endindex check for off by one
                    .Select(x => x.AsReadOnlyBuffer().SetOrder(ByteOrder.LittleEndian))
                    .ToList();

                if (endOffset == 0)
                {
                    cloned.Add(ByteBuffer.Allocate(0).SetOrder(ByteOrder.LittleEndian));
                }

                cloned[0].SetPosition(Convert.ToInt32(absStart & blockMask));
                cloned[cloned.Count - 1].SetLimit(endOffset);
                return cloned;
            }
        }
    }
}
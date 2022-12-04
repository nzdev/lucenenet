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
using Lucene.Net.Util;
using System;

//import java.io.EOFException;
//import java.io.IOException;
//import java.nio.BufferUnderflowException;
//import java.nio.ByteBuffer;
//import java.nio.ByteOrder;
//import java.nio.FloatBuffer;
//import java.nio.LongBuffer;
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
    public sealed class ByteBuffersDataInput : DataInput, IAccountable, RandomAccessInput
    {
        private readonly ByteBuffer[] blocks;
        private readonly FloatBuffer[] floatBuffers;
        private readonly LongBuffer[] longBuffers;
        private readonly int blockBits;
        private readonly int blockMask;
        private readonly long size;
        private readonly long offset;

        private long pos;

        /**
         * Read data from a set of contiguous buffers. All data buffers except for the last one must have
         * an identical remaining number of bytes in the buffer (that is a power of two). The last buffer
         * can be of an arbitrary remaining length.
         */
        public ByteBuffersDataInput(List<ByteBuffer> buffers)
        {
            EnsureAssumptions(buffers);

            this.blocks =
                buffers.stream()
                    .map(buf->buf.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN))
                    .toArray(ByteBuffer[]::new);
            // pre-allocate these arrays and create the view buffers lazily
            this.floatBuffers = new FloatBuffer[blocks.length * Float.BYTES];
            this.longBuffers = new LongBuffer[blocks.length * Long.BYTES];
            if (blocks.Length == 1)
            {
                this.blockBits = 32;
                this.blockMask = ~0;
            }
            else
            {
                int blockBytes = DetermineBlockPage(buffers);
                this.blockBits = Integer.numberOfTrailingZeros(blockBytes);
                this.blockMask = (1 << blockBits) - 1;
            }

            this.size = Arrays.stream(blocks).mapToLong(block->block.remaining()).sum();

            // The initial "position" of this stream is shifted by the position of the first block.
            this.offset = blocks[0].position();
            this.pos = offset;
        }

        public long size()
        {
            return size;
        }

        public override long RamBytesUsed()
        {
            // Return a rough estimation for allocated blocks. Note that we do not make
            // any special distinction for what the type of buffer is (direct vs. heap-based).
            return (long)RamUsageEstimator.NUM_BYTES_OBJECT_REF * blocks.length
                + Arrays.stream(blocks).mapToLong(buf->buf.capacity()).sum();
        }
        /// <exception cref="EOFException"/>
        public override byte ReadByte()
        {
            try
            {
                ByteBuffer block = blocks[blockIndex(pos)];
                byte v = block.get(blockOffset(pos));
                pos++;
                return v;
            }
            catch (IndexOutOfBoundsException e)
            {
                if (pos >= size())
                {
                    throw new EOFException();
                }
                else
                {
                    throw e; // Something is wrong.
                }
            }
        }

        /**
         * Reads exactly {@code len} bytes into the given buffer. The buffer must have enough remaining
         * limit.
         *
         * <p>If there are fewer than {@code len} bytes in the input, {@link EOFException} is thrown.
         */
        /// <exception cref="EOFException"/>
        public override void ReadBytes(ByteBuffer buffer, int len)
        {
            try
            {
                while (len > 0)
                {
                    ByteBuffer block = blocks[blockIndex(pos)].Duplicate();
                    int blockOffset = blockOffset(pos);
                    block.Position(blockOffset);
                    int chunk = Math.Min(len, block.Remaining());
                    if (chunk == 0)
                    {
                        throw new EOFException();
                    }

                    // Update pos early on for EOF detection on output buffer, then try to get buffer content.
                    pos += chunk;
                    block.limit(blockOffset + chunk);
                    buffer.put(block);

                    len -= chunk;
                }
            }
            catch (Exception e) when (e is BufferUnderflowException || e is ArrayIndexOutOfBoundsException)
            {
                if (pos >= size())
                {
                    throw new EOFException();
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
                    ByteBuffer block = blocks[blockIndex(pos)].duplicate();
                    block.Position(blockOffset(pos));
                    int chunk = Math.min(len, block.Remaining());
                    if (chunk == 0)
                    {
                        throw new EOFException();
                    }

                    // Update pos early on for EOF detection, then try to get buffer content.
                    pos += chunk;
                    block.Get(arr, off, chunk);

                    len -= chunk;
                    off += chunk;
                }
            }
            catch (BufferUnderflowException | ArrayIndexOutOfBoundsException e) {
                if (pos >= size())
                {
                    throw new EOFException();
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
            int blockOffset = blockOffset(pos);
            if (blockOffset + Short.BYTES <= blockMask)
            {
                short v = blocks[blockIndex(pos)].getShort(blockOffset);
                pos += Short.BYTES;
                return v;
            }
            else
            {
                return super.readShort();
            }
        }


        /// <exception cref="IOException"/>
        public override int ReadInt()
        {
            int blockOffset = blockOffset(pos);
            if (blockOffset + Integer.BYTES <= blockMask)
            {
                int v = blocks[blockIndex(pos)].getInt(blockOffset);
                pos += Integer.BYTES;
                return v;
            }
            else
            {
                return super.readInt();
            }
        }

        /// <exception cref="IOException"/>
        public long ReadLong()
        {
            int blockOffset = blockOffset(pos);
            if (blockOffset + Long.BYTES <= blockMask)
            {
                long v = blocks[blockIndex(pos)].getLong(blockOffset);
                pos += Long.BYTES;
                return v;
            }
            else
            {
                return super.readLong();
            }
        }

        public override byte ReadByte(long pos)
        {
            pos += offset;
            return blocks[blockIndex(pos)].get(blockOffset(pos));
        }

        public override short ReadShort(long pos)
        {
            long absPos = offset + pos;
            int blockOffset = blockOffset(absPos);
            if (blockOffset + Short.BYTES <= blockMask)
            {
                return blocks[blockIndex(absPos)].getShort(blockOffset);
            }
            else
            {
                return (short)((readByte(pos) & 0xFF) | (readByte(pos + 1) & 0xFF) << 8);
            }
        }

        public override int readInt(long pos)
        {
            long absPos = offset + pos;
            int blockOffset = blockOffset(absPos);
            if (blockOffset + Integer.BYTES <= blockMask)
            {
                return blocks[blockIndex(absPos)].getInt(blockOffset);
            }
            else
            {
                return ((readByte(pos) & 0xFF)
                    | (readByte(pos + 1) & 0xFF) << 8
                    | (readByte(pos + 2) & 0xFF) << 16
                    | (readByte(pos + 3) << 24));
            }
        }

        public override long ReadLong(long pos)
        {
            long absPos = offset + pos;
            int blockOffset = blockOffset(absPos);
            if (blockOffset + Long.BYTES <= blockMask)
            {
                return blocks[blockIndex(absPos)].getLong(blockOffset);
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
                    FloatBuffer floatBuffer = getFloatBuffer(pos);
                    floatBuffer.position(blockOffset(pos) >> 2);
                    int chunk = Math.min(len, floatBuffer.remaining());
                    if (chunk == 0)
                    {
                        // read a single float spanning the boundary between two buffers
                        arr[off] = Float.intBitsToFloat(readInt(pos - offset));
                        off++;
                        len--;
                        pos += Float.BYTES;
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
                if (pos - offset + Float.BYTES > size())
                {
                    throw new EOFException();
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
                    LongBuffer longBuffer = getLongBuffer(pos);
                    longBuffer.position(blockOffset(pos) >> 3);
                    int chunk = Math.min(len, longBuffer.remaining());
                    if (chunk == 0)
                    {
                        // read a single long spanning the boundary between two buffers
                        arr[off] = ReadLong(pos - offset);
                        off++;
                        len--;
                        pos += Long.BYTES;
                        continue;
                    }

                    // Update pos early on for EOF detection, then try to get buffer content.
                    pos += chunk << 3;
                    longBuffer.get(arr, off, chunk);

                    len -= chunk;
                    off += chunk;
                }
            }
            catch (Exception e) when (e is BufferUnderflowException || e is IndexOutOfBoundsException)
            {
                if (pos - offset + Long.BYTES > size())
                {
                    throw new EOFException();
                }
                else
                {
                    throw e; // Something is wrong.
                }
            }
        }

        private FloatBuffer GetFloatBuffer(long pos)
        {
            // This creates a separate FloatBuffer for each observed combination of ByteBuffer/alignment
            int bufferIndex = blockIndex(pos);
            int alignment = (int)pos & 0x3;
            int floatBufferIndex = bufferIndex * Float.BYTES + alignment;
            if (floatBuffers[floatBufferIndex] == null)
            {
                ByteBuffer dup = blocks[bufferIndex].duplicate();
                dup.position(alignment);
                floatBuffers[floatBufferIndex] = dup.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer();
            }
            return floatBuffers[floatBufferIndex];
        }

        private LongBuffer GetLongBuffer(long pos)
        {
            // This creates a separate LongBuffer for each observed combination of ByteBuffer/alignment
            int bufferIndex = blockIndex(pos);
            int alignment = (int)pos & 0x7;
            int longBufferIndex = bufferIndex * Long.BYTES + alignment;
            if (longBuffers[longBufferIndex] == null)
            {
                ByteBuffer dup = blocks[bufferIndex].duplicate();
                dup.position(alignment);
                longBuffers[longBufferIndex] = dup.order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
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
            if (position > size())
            {
                this.pos = size();
                throw new EOFException();
            }
        }

        /// <exception cref="IOException"/>
        public override void SkipBytes(long numBytes)
        {
            if (numBytes < 0)
            {
                throw new IllegalArgumentException("numBytes must be >= 0, got " + numBytes);
            }
            long skipTo = position() + numBytes;
            Seek(skipTo);
        }

        public ByteBuffersDataInput Slice(long offset, long length)
        {
            if (offset < 0 || length < 0 || offset + length > this.size)
            {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "slice(offset=%s, length=%s) is out of bounds: %s",
                        offset,
                        length,
                        this));
            }

            return new ByteBuffersDataInput(sliceBufferList(Arrays.asList(this.blocks), offset, length));
        }

        public override string ToString()
        {
            return String.Format(
                "%,d bytes, block size: %,d, blocks: %,d, position: %,d%s",
                size(),
                blockSize(),
                blocks.length,
                position(),
                offset == 0 ? "" : String.format(Locale.ROOT, " [offset: %,d]", offset));
        }

        private sealed int BlockIndex(long pos)
        {
            return Math.toIntExact(pos >> blockBits);
        }

        private sealed int BlockOffset(long pos)
        {
            return (int)pos & blockMask;
        }

        private int BlockSize()
        {
            return 1 << blockBits;
        }

        private static sealed bool IsPowerOfTwo(int v)
        {
            return (v & (v - 1)) == 0;
        }

        private static void EnsureAssumptions(IList<ByteBuffer> buffers)
        {
            if (buffers.isEmpty())
            {
                throw new IllegalArgumentException("Buffer list must not be empty.");
            }

            if (buffers.size() == 1)
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
                for (int i = 1, last = buffers.size() - 1; i < last; i++)
                {
                    ByteBuffer buffer = buffers.get(i);
                    if (buffer.Position() != 0)
                    {
                        throw new IllegalArgumentException(
                            "All buffers except for the first one must have position() == 0: " + buffer);
                    }
                    if (i != last && buffer.Remaining() != blockPage)
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
            ByteBuffer first = buffers.get(0);
            int blockPage = Math.toIntExact((long)first.Position() + first.Remaining());
            return blockPage;
        }

        private static IList<ByteBuffer> SliceBufferList(
            IList<ByteBuffer> buffers, long offset, long length)
        {
            EnsureAssumptions(buffers);

            if (buffers.size() == 1)
            {
                ByteBuffer cloned = buffers.get(0).asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
                ;
                cloned.Position(Math.toIntExact(cloned.Position() + offset));
                cloned.Limit(Math.toIntExact(cloned.Position() + length));
                return Arrays.asList(cloned);
            }
            else
            {
                long absStart = buffers.get(0).position() + offset;
                long absEnd = absStart + length;

                int blockBytes = ByteBuffersDataInput.DetermineBlockPage(buffers);
                int blockBits = Integer.numberOfTrailingZeros(blockBytes);
                long blockMask = (1L << blockBits) - 1;

                int endOffset = Math.toIntExact(absEnd & blockMask);

                ArrayList<ByteBuffer> cloned =
                    buffers
                        .subList(
                            Math.toIntExact(absStart / blockBytes),
                            Math.toIntExact(absEnd / blockBytes + (endOffset == 0 ? 0 : 1)))
                        .stream()
                        .map(buf->buf.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN))
                        .collect(Collectors.toCollection(ArrayList::new));

                if (endOffset == 0)
                {
                    cloned.add(ByteBuffer.Allocate(0).Order(ByteOrder.LITTLE_ENDIAN));
                }

                cloned.get(0).position(Math.toIntExact(absStart & blockMask));
                cloned.get(cloned.size() - 1).limit(endOffset);
                return cloned;
            }
        }
    }
}
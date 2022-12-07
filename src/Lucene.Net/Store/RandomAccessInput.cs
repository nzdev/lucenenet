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
using System.IO;

namespace Lucene.Net.Store
{
    /// <summary>
    /// Random Access Index API. Unlike {@link IndexInput}, this has no concept of file position, all
    /// reads are absolute. However, like IndexInput, it is only intended for use by a single thread.
    /// </summary>
    public interface IRandomAccessInput
    {
        /// <summary>
        /// Reads a byte at the given position in the file
        /// @see DataInput#readByte
        /// </summary>
        /// <exception cref="IOException"/>
        public byte ReadByte(long pos);

        /// <summary>
        /// Reads a short (LE byte order) at the given position in the file
        /// @see DataInput#readShort
        /// @see BitUtil#VH_LE_SHORT
        /// </summary>
        /// <exception cref="IOException"/>
        public short ReadShort(long pos);

        /// <summary>
        /// Reads an integer (LE byte order) at the given position in the file
        /// @see DataInput#readInt
        /// @see BitUtil#VH_LE_INT
        /// </summary>
        /// <exception cref="IOException"/>
        public int ReadInt(long pos);

        /// <summary>
        /// Reads a long (LE byte order) at the given position in the file
        /// @see DataInput#readLong
        /// @see BitUtil#VH_LE_LONG
        /// </summary>
        /// <exception cref="IOException"/>
        public long ReadLong(long pos);
    }
}
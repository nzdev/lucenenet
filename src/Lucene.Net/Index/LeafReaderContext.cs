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
using Lucene.Net.Support.Threading;
using System.Collections.Generic;

namespace Lucene.Net.Index
{


    /** {@link IndexReaderContext} for {@link LeafReader} instances. */
    public sealed class LeafReaderContext : IndexReaderContext
    {
        /** The reader's ord in the top-level's leaves array */
        public readonly int ord;
        /** The reader's absolute doc base */
        public readonly int docBase;

        private readonly LeafReader reader;
        private readonly IList<LeafReaderContext> leaves;

       

        /** Creates a new {@link LeafReaderContext} */
        public LeafReaderContext(
            CompositeReaderContext parent,
            LeafReader reader,
            int ord,
            int docBase,
            int leafOrd,
            int leafDocBase) : base(parent, ord, docBase)
        {
            this.ord = leafOrd;
            this.docBase = leafDocBase;
            this.reader = reader;
            this.leaves = isTopLevel ? Collections.singletonList(this) : null;
        }

        public LeafReaderContext(LeafReader leafReader)
        {
            LeafReaderContext(null, leafReader, 0, 0, 0, 0);
        }
        //override
        public new IList<LeafReaderContext> Leaves()
        {
            if (!isTopLevel)
            {
                throw new UnsupportedOperationException("This is not a top-level context.");
            }
            if (Debugging.AssertsEnabled)
            {
                Debugging.Assert(leaves != null);
            }
            return leaves;
        }
        //override
        public override IList<IndexReaderContext> Children => null;

        //override
        public override LeafReader Reader => reader;

        public override string ToString()
        {
            return "LeafReaderContext(" + reader + " docBase=" + docBase + " ord=" + ord + ")";
        }
    }
}
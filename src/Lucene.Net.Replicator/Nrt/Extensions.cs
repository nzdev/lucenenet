using Lucene.Net.Index;
using Lucene.Net.Search;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lucene.Net.Replicator.Nrt
{
    internal static class Extensions
    {
        internal static IndexReader GetIndexReader(this IndexSearcher indexSearcher)
        {
            return indexSearcher.IndexReader;
        }
    }
}

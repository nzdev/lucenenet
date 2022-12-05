using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using System;
using System.IO;
using Directory = Lucene.Net.Store.Directory;

namespace Lucene.Net.Replicator.Nrt
{
    //LUCENENET Only
    internal static class Extensions
    {
        internal static IndexReader GetIndexReader(this IndexSearcher indexSearcher)
        {
            return indexSearcher.IndexReader;
        }

        internal static Lock ObtainLock(this Directory directory, string lockName)
        {
            return directory.LockFactory.MakeLock(lockName);
        }

        internal static void PrintStackTrace(this Exception e, TextWriter destination)
        {
            destination.WriteLine(e.StackTrace);
        }

        internal static double TimeUnitSecondsToNanos(int num)
        {
            //TimeUnit.SECONDS.toNanos(1)
            throw new NotImplementedException();
        }

        internal static int LongBytes()
        {
            //Long.BYTES
            return 8;
        }
    }
}

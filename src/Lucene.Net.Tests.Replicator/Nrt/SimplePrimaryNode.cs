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
//import java.io.BufferedOutputStream;
//import java.io.EOFException;
//import java.io.IOException;
//import java.io.InputStream;
//import java.net.ServerSocket;
//import java.net.Socket;
//import java.nio.file.Path;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Locale;
//import java.util.Map;
//import java.util.Random;
//import java.util.Set;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicBoolean;
//import org.apache.lucene.document.Document;
//import org.apache.lucene.document.Field;
//import org.apache.lucene.document.FieldType;
//import org.apache.lucene.document.StringField;
//import org.apache.lucene.document.TextField;
//import org.apache.lucene.index.DirectoryReader;
//import org.apache.lucene.index.IndexOptions;
//import org.apache.lucene.index.IndexWriter;
//import org.apache.lucene.index.IndexWriterConfig;
//import org.apache.lucene.index.LogMergePolicy;
//import org.apache.lucene.index.MergePolicy;
//import org.apache.lucene.index.SegmentCommitInfo;
//import org.apache.lucene.index.Term;
//import org.apache.lucene.index.TieredMergePolicy;
//import org.apache.lucene.search.IndexSearcher;
//import org.apache.lucene.search.MatchAllDocsQuery;
//import org.apache.lucene.search.ScoreDoc;
//import org.apache.lucene.search.SearcherFactory;
//import org.apache.lucene.search.TermQuery;
//import org.apache.lucene.search.TopDocs;
//import org.apache.lucene.store.DataInput;
//import org.apache.lucene.store.DataOutput;
//import org.apache.lucene.store.Directory;
//import org.apache.lucene.store.IOContext;
//import org.apache.lucene.store.IndexInput;
//import org.apache.lucene.tests.analysis.MockAnalyzer;
//import org.apache.lucene.tests.util.LuceneTestCase;
//import org.apache.lucene.tests.util.TestUtil;
//import org.apache.lucene.util.IOUtils;
//import org.apache.lucene.util.ThreadInterruptedException;

using J2N.IO;
using Lucene;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Replicator.Nrt;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;
using System;
using System.Collections;
using System.Threading;

namespace Lucene.Net.Replicator.Nrt
{

    /** A primary node that uses simple TCP connections to send commands and copy files */
    class SimplePrimaryNode : PrimaryNode
    {

        readonly int tcpPort;

        readonly Random random;

        // These are updated by parent test process whenever replicas change:
        int[] replicaTCPPorts = new int[0];
        int[] replicaIDs = new int[0];

        // So we only flip a bit once per file name:
        readonly ISet<string> bitFlipped = Collections.synchronizedSet(new HashSet<>());

        readonly IList<MergePreCopy> warmingSegments = Collections.synchronizedList(new ArrayList<>());

        readonly bool doFlipBitsDuringCopy;

        static class MergePreCopy
        {
            readonly IList<Connection> connections = Collections.synchronizedList(new ArrayList<>());
            readonly IDictionary<String, FileMetaData> files;
            private bool finished;

            public MergePreCopy(IDictionary<string, FileMetaData> files)
            {
                this.files = files;
            }

            public synchronized bool TryAddConnection(Connection c)
            {
                if (finished == false)
                {
                    connections.add(c);
                    return true;
                }
                else
                {
                    return false;
                }
            }

            public synchronized bool Finished()
            {
                if (connections.isEmpty())
                {
                    finished = true;
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        /// <exception cref="IOException"/>
        public SimplePrimaryNode(
    Random random,
    Path indexPath,
    int id,
    int tcpPort,
    long primaryGen,
    long forcePrimaryVersion,
    SearcherFactory searcherFactory,
    bool doFlipBitsDuringCopy,
    bool doCheckIndexOnClose)
        {
            super(
                initWriter(id, random, indexPath, doCheckIndexOnClose),
                id,
                primaryGen,
                forcePrimaryVersion,
                searcherFactory,
                System.out);
            this.tcpPort = tcpPort;
            this.random = new Random(random.nextLong());
            this.doFlipBitsDuringCopy = doFlipBitsDuringCopy;
        }

        /** Records currently alive replicas. */
        public synchronized void setReplicas(int[] replicaIDs, int[] replicaTCPPorts)
        {
            message(
                "top: set replicasIDs="
                    + Arrays.toString(replicaIDs)
                    + " tcpPorts="
                    + Arrays.toString(replicaTCPPorts));
            this.replicaIDs = replicaIDs;
            this.replicaTCPPorts = replicaTCPPorts;
        }

        /// <exception cref="IOException"/>
        private static IndexWriter initWriter(int id, Random random, Path indexPath, boolean doCheckIndexOnClose)
        {
            Directory dir = SimpleReplicaNode.GetDirectory(random, id, indexPath, doCheckIndexOnClose);

            MockAnalyzer analyzer = new MockAnalyzer(random);
            analyzer.setMaxTokenLength(TestUtil.nextInt(random, 1, IndexWriter.MAX_TERM_LENGTH));
            IndexWriterConfig iwc = LuceneTestCase.newIndexWriterConfig(random, analyzer);

            MergePolicy mp = iwc.getMergePolicy();
            // iwc.setInfoStream(new PrintStreamInfoStream(System.out));

            // Force more frequent merging so we stress merge warming:
            if (mp is TieredMergePolicy)
            {
                TieredMergePolicy tmp = (TieredMergePolicy)mp;
                tmp.SetSegmentsPerTier(3);
                tmp.SetMaxMergeAtOnce(3);
            }
            else if (mp is LogMergePolicy)
            {
                LogMergePolicy lmp = (LogMergePolicy)mp;
                lmp.setMergeFactor(3);
            }

            IndexWriter writer = new IndexWriter(dir, iwc);

            TestUtil.reduceOpenFiles(writer);
            return writer;
        }


        /// <exception cref="IOException"/>
        protected override void preCopyMergedSegmentFiles(SegmentCommitInfo info, IDictionary<String, FileMetaData> files)
        {
            int[]
            replicaTCPPorts = this.replicaTCPPorts;
            if (replicaTCPPorts == null)
            {
                Message("no replicas; skip warming " + info);
                return;
            }

            Message(
                "top: warm merge "
                    + info
                    + " to "
                    + replicaTCPPorts.length
                    + " replicas; tcpPort="
                    + tcpPort
                    + ": files="
                    + files.keySet());

            MergePreCopy preCopy = new MergePreCopy(files);
            warmingSegments.add(preCopy);

            try
            {
                // Ask all currently known replicas to pre-copy this newly merged segment's files:
                foreach (int replicaTCPPort in replicaTCPPorts)
                {
                    try
                    {
                        Connection c = new Connection(replicaTCPPort);
                        c.output.writeByte(SimpleReplicaNode.CMD_PRE_COPY_MERGE);
                        c.output.writeVLong(primaryGen);
                        c.output.writeVInt(tcpPort);
                        TestSimpleServer.writeFilesMetaData(c.output, files);
                        c.flush();
                        c.s.shutdownOutput();
                        message("warm connection " + c.s);
                        preCopy.connections.add(c);
                    }
                    catch (Throwable t)
                    {
                        Message(
                            "top: ignore exception trying to warm to replica port " + replicaTCPPort + ": " + t);
                        // t.printStackTrace(System.out);
                    }
                }

                long startNS = System.nanoTime();
                long lastWarnNS = startNS;

                // TODO: maybe ... place some sort of time limit on how long we are willing to wait for slow
                // replica(s) to finish copying?
                while (preCopy.Finished() == false)
                {
                    try
                    {
                        Thread.Sleep(10);
                    }
                    catch (InterruptedException ie)
                    {
                        throw new ThreadInterruptedException(ie);
                    }

                    if (isClosed())
                    {
                        Message("top: primary is closing: now cancel segment warming");
                        synchronized(preCopy.connections) {
                            IOUtils.closeWhileHandlingException(preCopy.connections);
                        }
                        return;
                    }

                    long ns = System.nanoTime();
                    if (ns - lastWarnNS > 1000000000L)
                    {
                        message(
                            String.format(
                                Locale.ROOT,
                                "top: warning: still warming merge "
                                    + info
                                    + " to "
                                    + preCopy.connections.size()
                                    + " replicas for %.1f sec...",
                                (ns - startNS) / (double)TimeUnit.SECONDS.toNanos(1)));
                        lastWarnNS = ns;
                    }

                    // Process keep-alives:
                    synchronized(preCopy.connections) {
                        Iterator<Connection> it = preCopy.connections.iterator();
                        while (it.hasNext())
                        {
                            Connection c = it.next();
                            try
                            {
                                long nowNS = Time.NanoTime();
                                bool done = false;
                                while (c.sockIn.available() > 0)
                                {
                                    byte b = c.input.readByte();
                                    if (b == 0)
                                    {
                                        // keep-alive
                                        c.lastKeepAliveNS = nowNS;
                                        message("keep-alive for socket=" + c.s + " merge files=" + files.keySet());
                                    }
                                    else
                                    {
                                        // merge is done pre-copying to this node
                                        if (b != 1)
                                        {
                                            throw new IllegalArgumentException();
                                        }
                                        message(
                                            "connection socket="
                                                + c.s
                                                + " is done warming its merge "
                                                + info
                                                + " files="
                                                + files.keySet());
                                        IOUtils.CloseWhileHandlingException(c);
                                        it.remove();
                                        done = true;
                                        break;
                                    }
                                }

                                // If > 2 sec since we saw a keep-alive, assume this replica is dead:
                                if (done == false && nowNS - c.lastKeepAliveNS > 2000000000L)
                                {
                                    message(
                                        "top: warning: replica socket="
                                            + c.s
                                            + " for segment="
                                            + info
                                            + " seems to be dead; closing files="
                                            + files.keySet());
                                    IOUtils.CloseWhileHandlingException(c);
                                    it.remove();
                                    done = true;
                                }

                                if (done == false && random.nextInt(1000) == 17)
                                {
                                    message(
                                        "top: warning: now randomly dropping replica from merge warming; files="
                                            + files.keySet());
                                    IOUtils.CloseWhileHandlingException(c);
                                    it.remove();
                                    done = true;
                                }

                            }
                            catch (Exception t)
                            {
                                message(
                                    "top: ignore exception trying to read byte during warm for segment="
                                        + info
                                        + " to replica socket="
                                        + c.s
                                        + ": "
                                        + t
                                        + " files="
                                        + files.keySet());
                                IOUtils.CloseWhileHandlingException(c);
                                it.remove();
                            }
                        }
                    }
                }
            }
            finally
            {
                warmingSegments.remove(preCopy);
            }
        }

        /** Flushes all indexing ops to disk and notifies all replicas that they should now copy */
        /// <exception cref="IOException"/>
        private void handleFlush(DataInput topIn, DataOutput topOut, BufferedOutputStream bos)
        {
            Thread.CurrentThread.Name = ("flush");

            int atLeastMarkerCount = topIn.readVInt();

            int[]
            replicaTCPPorts;
            int[]
            replicaIDs;
            synchronized(this)
        {
                replicaTCPPorts = this.replicaTCPPorts;
                replicaIDs = this.replicaIDs;
            }

            Message("now flush; " + replicaIDs.length + " replicas");

            if (FlushAndRefresh())
            {
                // Something did get flushed (there were indexing ops since the last flush):

                VerifyAtLeastMarkerCount(atLeastMarkerCount, null);

                // Tell caller the version before pushing to replicas, so that even if we crash after this,
                // caller will know what version we
                // (possibly) pushed to some replicas.  Alternatively we could make this 2 separate ops?
                long version = GetCopyStateVersion();
                message("send flushed version=" + version);
                topOut.writeLong(version);
                bos.flush();

                // Notify current replicas:
                for (int i = 0; i < replicaIDs.length; i++)
                {
                    int replicaID = replicaIDs[i];
                    try (Connection c = new Connection(replicaTCPPorts[i])) {
                    message("send NEW_NRT_POINT to R" + replicaID + " at tcpPort=" + replicaTCPPorts[i]);
                    c.output.writeByte(SimpleReplicaNode.CMD_NEW_NRT_POINT);
                    c.output.writeVLong(version);
                    c.output.writeVLong(primaryGen);
                    c.output.writeInt(tcpPort);
                    c.flush();
                    // TODO: we should use multicast to broadcast files out to replicas
                    // TODO: ... replicas could copy from one another instead of just primary
                    // TODO: we could also prioritize one replica at a time?
                } catch (Exception t)
                {
                    Message(
                        "top: failed to connect R"
                            + replicaID
                            + " for newNRTPoint; skipping: "
                            + t.getMessage());
                }
            }
        } else
{
    // No changes flushed:
    topOut.writeLong(-getCopyStateVersion());
}
}

/** Pushes CopyState on the wire */
/// <exception cref="IOException"/>
private static void WriteCopyState(CopyState state, DataOutput output)
{
    // TODO (opto): we could encode to byte[] once when we created the copyState, and then just send
    // same byts to all replicas...
    output.writeVInt(state.infosBytes.length);
    output.writeBytes(state.infosBytes, 0, state.infosBytes.length);
    output.writeVLong(state.gen);
    output.writeVLong(state.version);
    TestSimpleServer.writeFilesMetaData(output, state.files);

    output.writeVInt(state.completedMergeFiles.size());
    for (String fileName : state.completedMergeFiles) {
    output.writeString(fileName);
}
output.writeVLong(state.primaryGen);
}

/** Called when another node (replica) wants to copy files from us */
/// <exception cref="IOException"/>
private bool handleFetchFiles(Random random, Socket socket, DataInput destIn, DataOutput destOut, BufferedOutputStream bos)
{
    Thread.CurrentThread.Name = ("send");

    int replicaID = destIn.readVInt();
    message("top: start fetch for R" + replicaID + " socket=" + socket);
    byte b = destIn.readByte();
    CopyState copyState;
    if (b == 0)
    {
        // Caller already has CopyState
        copyState = null;
    }
    else if (b == 1)
    {
        // Caller does not have CopyState; we pull the latest one:
        copyState = getCopyState();
        Thread.currentThread().setName("send-R" + replicaID + "-" + copyState.version);
    }
    else
    {
        // Protocol error:
        throw new IllegalArgumentException("invalid CopyState byte=" + b);
    }

    try
    {
        if (copyState != null)
        {
            // Serialize CopyState on the wire to the client:
            writeCopyState(copyState, destOut);
            bos.flush();
        }

        byte[] buffer = new byte[16384];
        int fileCount = 0;
        long totBytesSent = 0;
        while (true)
        {
            byte done = destIn.readByte();
            if (done == 1)
            {
                break;
            }
            else if (done != 0)
            {
                throw new IllegalArgumentException("expected 0 or 1 byte but got " + done);
            }

            // Name of the file the replica wants us to send:
            string fileName = destIn.ReadString();

            // Starting offset in the file we should start sending bytes from:
            long fpStart = destIn.readVLong();

            using (IndexInput input = dir.openInput(fileName, IOContext.DEFAULT))
            {
                long len = input.length();
                // message("fetch " + fileName + ": send len=" + len);
                destOut.writeVLong(len);
                input.seek(fpStart);
                long upto = fpStart;
                while (upto < len)
                {
                    int chunk = (int)Math.min(buffer.length, (len - upto));
                    input.readBytes(buffer, 0, chunk);
                    if (doFlipBitsDuringCopy)
                    {
                        if (random.nextInt(3000) == 17 && bitFlipped.contains(fileName) == false)
                        {
                            bitFlipped.add(fileName);
                            message(
                                "file "
                                    + fileName
                                    + " to R"
                                    + replicaID
                                    + ": now randomly flipping a bit at byte="
                                    + upto);
                            int x = random.nextInt(chunk);
                            int bit = random.nextInt(8);
                            buffer[x] ^= 1 << bit;
                        }
                    }
                    destOut.writeBytes(buffer, 0, chunk);
                    upto += chunk;
                    totBytesSent += chunk;
                }
            }

            fileCount++;
        }

        Message(
            "top: done fetch files for R"
                + replicaID
                + ": sent "
                + fileCount
                + " files; sent "
                + totBytesSent
                + " bytes");
    }
    catch (Throwable t)
    {
        message("top: exception during fetch: " + t.getMessage() + "; now close socket");
        socket.close();
        return false;
    }
    finally
    {
        if (copyState != null)
        {
            message("top: fetch: now release CopyState");
            releaseCopyState(copyState);
        }
    }

    return true;
}

static readonly FieldType tokenizedWithTermVectors;

static
{
    tokenizedWithTermVectors = new FieldType(TextField.TYPE_STORED);
    tokenizedWithTermVectors.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    tokenizedWithTermVectors.setStoreTermVectors(true);
    tokenizedWithTermVectors.setStoreTermVectorOffsets(true);
    tokenizedWithTermVectors.setStoreTermVectorPositions(true);
}

/// <exception cref="IOException"/>
/// <exception cref="InterruptedException"/>
private void handleIndexing(
    Socket socket,
    AtomicBoolean stop,
    InputStream inputstream,
    DataInput datainput,
    DataOutput dataoutput,
    BufferedOutputStream bos)
{
    Thread.CurrentThread.Name = ("indexing");
    Message("start handling indexing socket=" + socket);
    while (true)
    {
        while (true)
        {
            if (inputstream.available() > 0)
            {
                break;
            }
            if (stop.get())
            {
                return;
            }
            Thread.Sleep(10);
        }
        byte cmd;
        try
        {
            cmd = datainput.ReadByte();
        }
        catch (
              // @SuppressWarnings("unused")
              EOFException eofe)
        {
            // done
            return;
        }
        // message("INDEXING OP " + cmd);
        if (cmd == CMD_ADD_DOC)
        {
            handleAddDocument(datainput, dataoutput);
            dataoutput.writeByte((byte)1);
            bos.flush();
        }
        else if (cmd == CMD_UPDATE_DOC)
        {
            handleUpdateDocument(datainput, dataoutput);
            dataoutput.WriteByte((byte)1);
            bos.flush();
        }
        else if (cmd == CMD_DELETE_DOC)
        {
            handleDeleteDocument(datainput, dataoutput);
            dataoutput.WriteByte((byte)1);
            bos.flush();
        }
        else if (cmd == CMD_DELETE_ALL_DOCS)
        {
            writer.deleteAll();
            dataoutput.WriteByte((byte)1);
            bos.flush();
        }
        else if (cmd == CMD_FORCE_MERGE)
        {
            writer.forceMerge(1);
            dataoutput.WriteByte((byte)1);
            bos.flush();
        }
        else if (cmd == CMD_INDEXING_DONE)
        {
            dataoutput.WriteByte((byte)1);
            bos.flush();
            break;
        }
        else
        {
            throw new IllegalArgumentException("cmd must be add, update or delete; got " + cmd);
        }
    }
}
/// <exception cref="IOException"/>
private void handleAddDocument(DataInput input, DataOutput output)
{
    int fieldCount = input.ReadVInt();
    Document doc = new Document();
    for (int i = 0; i < fieldCount; i++)
    {
        string name = input.ReadString();
        string value = input.ReadString();
        // NOTE: clearly NOT general!
        if (name.Equals("docid") || name.Equals("marker"))
        {
            doc.Add(new StringField(name, value, Field.Store.YES));
        }
        else if (name.Equals("title"))
        {
            doc.Add(new StringField("title", value, Field.Store.YES));
            doc.Add(new Field("titleTokenized", value, tokenizedWithTermVectors));
        }
        else if (name.Equals("body"))
        {
            doc.Add(new Field("body", value, tokenizedWithTermVectors));
        }
        else
        {
            throw new IllegalArgumentException("unhandled field name " + name);
        }
    }
    writer.addDocument(doc);
}
/// <exception cref="IOException"/>
private void handleUpdateDocument(DataInput input, DataOutput output)
{
    int fieldCount = input.ReadVInt();
    Document doc = new Document();
    string docid = null;
    for (int i = 0; i < fieldCount; i++)
    {
        string name = input.ReadString();
        string value = input.ReadString();
        // NOTE: clearly NOT general!
        if (name.Equals("docid"))
        {
            docid = value;
            doc.Add(new StringField("docid", value, Field.Store.YES));
        }
        else if (name.Equals("marker"))
        {
            doc.Add(new StringField("marker", value, Field.Store.YES));
        }
        else if (name.Equals("title"))
        {
            doc.Add(new StringField("title", value, Field.Store.YES));
            doc.Add(new Field("titleTokenized", value, tokenizedWithTermVectors));
        }
        else if (name.Equals("body"))
        {
            doc.Add(new Field("body", value, tokenizedWithTermVectors));
        }
        else
        {
            throw new IllegalArgumentException("unhandled field name " + name);
        }
    }

    writer.updateDocument(new Term("docid", docid), doc);
}
/// <exception cref="IOException"/>
private void handleDeleteDocument(DataInput input, DataOutput output)
{
    string docid = input.ReadString();
    writer.deleteDocuments(new Term("docid", docid));
}

// Sent to primary to cutover new SIS:
static readonly byte CMD_FLUSH = 10;

// Sent by replica to primary asking to copy a set of files over:
static readonly byte CMD_FETCH_FILES = 1;
static readonly byte CMD_GET_SEARCHING_VERSION = 12;
static readonly byte CMD_SEARCH = 2;
static readonly byte CMD_MARKER_SEARCH = 3;
static readonly byte CMD_COMMIT = 4;
static readonly byte CMD_CLOSE = 5;
static readonly byte CMD_SEARCH_ALL = 21;

// Send (to primary) the list of currently running replicas:
static readonly byte CMD_SET_REPLICAS = 16;

// Multiple indexing ops
static readonly byte CMD_INDEXING = 18;
static readonly byte CMD_ADD_DOC = 6;
static readonly byte CMD_UPDATE_DOC = 7;
static readonly byte CMD_DELETE_DOC = 8;
static readonly byte CMD_INDEXING_DONE = 19;
static readonly byte CMD_DELETE_ALL_DOCS = 22;
static readonly byte CMD_FORCE_MERGE = 23;

// Sent by replica to primary when replica first starts up, so primary can add it to any warming
// merges:
static readonly byte CMD_NEW_REPLICA = 20;

// Leak a CopyState to simulate failure
static readonly byte CMD_LEAK_COPY_STATE = 24;
static readonly byte CMD_SET_CLOSE_WAIT_MS = 25;

/** Handles incoming request to the naive TCP server wrapping this node */
/// <exception cref="IOException"/>
/// /// <exception cref="InterruptedException"/>
void HandleOneConnection(
    Random random,
    ServerSocket ss,
    AtomicBoolean stop,
    InputStream inputstream,
    Socket socket,
    DataInput input,
    DataOutput output,
    BufferedOutputStream bos)
{

outer:
    while (true)
    {
        byte cmd;
        while (true)
        {
            if (inputstream.available() > 0)
            {
                break;
            }
            if (stop.get())
            {
                return;
            }
            Thread.sleep(10);
        }

        try
        {
            cmd = input.readByte();
        }
        catch (
              //SuppressWarnings("unused")
              EOFException eofe)
        {
            break;
        }

        switch (cmd)
        {
            case CMD_FLUSH:
                handleFlush(input, output, bos);
                break;

            case CMD_FETCH_FILES:
                // Replica (other node) is asking us (primary node) for files to copy
                handleFetchFiles(random, socket, input, output, bos);
                break;

            case CMD_INDEXING:
                handleIndexing(socket, stop, inputstream, input, output, bos);
                break;

            case CMD_GET_SEARCHING_VERSION:
                output.writeVLong(getCurrentSearchingVersion());
                break;

            case CMD_SEARCH:
                {
                    Thread.CurrentThread.Name = ("search");
                    IndexSearcher searcher = mgr.Acquire();
                    try
                    {
                        long version = ((DirectoryReader)searcher.GetIndexReader()).GetVersion();
                        int hitCount = searcher.Count(new TermQuery(new Term("body", "the")));
                        // message("version=" + version + " searcher=" + searcher);
                        output.writeVLong(version);
                        output.writeVInt(hitCount);
                        bos.flush();
                    }
                    finally
                    {
                        mgr.release(searcher);
                    }
                    bos.flush();
                }
                continue outer;

            case CMD_SEARCH_ALL:
                {
                    Thread.CurrentThread.Name = ("search all");
                    IndexSearcher searcher = mgr.acquire();
                    try
                    {
                        long version = ((DirectoryReader)searcher.GetIndexReader()).GetVersion();
                        int hitCount = searcher.count(new MatchAllDocsQuery());
                        // message("version=" + version + " searcher=" + searcher);
                        output.writeVLong(version);
                        output.writeVInt(hitCount);
                        bos.flush();
                    }
                    finally
                    {
                        mgr.release(searcher);
                    }
                }
                continue outer;

            case CMD_MARKER_SEARCH:
                {
                    Thread.CurrentThread.Name = ("msearch");
                    int expectedAtLeastCount = input.ReadVInt();
                    VerifyAtLeastMarkerCount(expectedAtLeastCount, output);
                    bos.flush();
                }
                continue outer;

            case CMD_COMMIT:
                Thread.CurrentThread.Name = ("commit");
                commit();
                output.writeByte((byte)1);
                break;

            case CMD_CLOSE:
                Thread.CurrentThread.Name = ("close");
                message("top close: now close server socket");
                ss.close();
                output.writeByte((byte)1);
                message("top close: done close server socket");
                break;

            case CMD_SET_REPLICAS:
                Thread.CurrentThread.Name = ("set repls");
                int count = input.ReadVInt();
                int[] replicaIDs = new int[count];
                int[] replicaTCPPorts = new int[count];
                for (int i = 0; i < count; i++)
                {
                    replicaIDs[i] = input.ReadVInt();
                    replicaTCPPorts[i] = input.ReadVInt();
                }
                output.writeByte((byte)1);
                setReplicas(replicaIDs, replicaTCPPorts);
                break;

            case CMD_NEW_REPLICA:
                Thread.CurrentThread.Name = ("new repl");
                int replicaTCPPort = input.ReadVInt();
                Message("new replica: " + warmingSegments.size() + " current warming merges");
                // Step through all currently warming segments and try to add this replica if it isn't
                // there already:
                synchronized(warmingSegments) {
                    foreach (MergePreCopy preCopy in warmingSegments)
                    {
                        Message("warming segment " + preCopy.files.keySet());
                        bool found = false;
                        synchronized(preCopy.connections) {
                            foreach (Connection c in preCopy.connections)
                            {
                                if (c.destTCPPort == replicaTCPPort)
                                {
                                    found = true;
                                    break;
                                }
                            }
                        }

                        if (found)
                        {
                            message("this replica is already warming this segment; skipping");
                            // It's possible (maybe) that the replica started up, then a merge kicked off, and
                            // it warmed to this new replica, all before the
                            // replica sent us this command:
                            continue;
                        }

                        // OK, this new replica is not already warming this segment, so attempt (could fail)
                        // to start warming now:

                        Connection c = new Connection(replicaTCPPort);
                        if (preCopy.tryAddConnection(c) == false)
                        {
                            // This can happen, if all other replicas just now finished warming this segment,
                            // and so we were just a bit too late.  In this
                            // case the segment will be copied over in the next nrt point sent to this replica
                            message("failed to add connection to segment warmer (too late); closing");
                            c.close();
                        }
                        c.output.writeByte(SimpleReplicaNode.CMD_PRE_COPY_MERGE);
                        c.output.writeVLong(primaryGen);
                        c.output.writeVInt(tcpPort);
                        TestSimpleServer.writeFilesMetaData(c.output, preCopy.files);
                        c.flush();
                        c.s.shutdownOutput();
                        message("successfully started warming");
                    }
                }
                break;

            case CMD_LEAK_COPY_STATE:
                message("leaking a CopyState");
                getCopyState();
                continue outer;

            case CMD_SET_CLOSE_WAIT_MS:
                setRemoteCloseTimeoutMs(input.readInt());
                continue outer;

            default:
                throw new IllegalArgumentException("unrecognized cmd=" + cmd + " via socket=" + socket);
        }
        bos.flush();
        break;
    }
}
/// <exception cref="IOException"/>
private void VerifyAtLeastMarkerCount(int expectedAtLeastCount, DataOutput output)
{
    IndexSearcher searcher = mgr.Acquire();
    try
    {
        long version = ((DirectoryReader)searcher.GetIndexReader()).GetVersion();
        int hitCount = searcher.Count(new TermQuery(new Term("marker", "marker")));

        if (hitCount < expectedAtLeastCount)
        {
            Message(
                "marker search: expectedAtLeastCount="
                    + expectedAtLeastCount
                    + " but hitCount="
                    + hitCount);
            TopDocs hits =
                searcher.Search(new TermQuery(new Term("marker", "marker")), expectedAtLeastCount);
            List<Integer> seen = new ArrayList<>();
            foreach (ScoreDoc hit in hits.ScoreDocs)
            {
                Document doc = searcher.Doc(hit.Doc);
                seen.Add(int.Parse(doc.Get("docid").Substring(1)));
            }
            Collections.sort(seen);
            Message("saw markers:");
            foreach (int marker in seen)
            {
                Message("saw m" + marker);
            }
            throw new IllegalStateException(
                "at flush: marker count "
                    + hitCount
                    + " but expected at least "
                    + expectedAtLeastCount
                    + " version="
                    + version);
        }

        if (output != null)
        {
            output.WriteVLong(version);
            output.WriteVInt(hitCount);
        }
    }
    finally
    {
        mgr.release(searcher);
    }
}
}

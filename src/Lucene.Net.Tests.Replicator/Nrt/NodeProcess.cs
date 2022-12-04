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


using System;
using System.IO;
using System.Threading;
using DotLiquid.Exceptions;
using Lucene.Net.Documents;
using Lucene.Net.Support.Threading;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.locks.ReentrantLock;
namespace Lucene.Net.Replicator.Nrt
{

    /// <summary>
    /// Parent JVM hold this "wrapper" to refer to each child JVM. This is roughly equivalent e.g.to a
    /// client-side "sugar" API.
    /// </summary>
    class NodeProcess : IDisposable
    {
        readonly Process p;

        // Port sub-process is listening on
        readonly int tcpPort;

        readonly int id;

        readonly Thread pumper;

        // Acquired when searching or indexing wants to use this node:
        readonly ReentrantLock lock;

        readonly bool isPrimary;

        // Version in the commit point we opened on init:
        readonly long initCommitVersion;

        // SegmentInfos.version, which can be higher than the initCommitVersion
        readonly long initInfosVersion;

        volatile bool isOpen = true;

        readonly AtomicBoolean nodeIsClosing;

        public NodeProcess(
            Process p,
            int id,
            int tcpPort,
            Thread pumper,
            boolean isPrimary,
            long initCommitVersion,
            long initInfosVersion,
            AtomicBoolean nodeIsClosing)
        {
            this.p = p;
            this.id = id;
            this.tcpPort = tcpPort;
            this.pumper = pumper;
            this.isPrimary = isPrimary;
            this.initCommitVersion = initCommitVersion;
            this.initInfosVersion = initInfosVersion;
            this.nodeIsClosing = nodeIsClosing;
            assert initInfosVersion >= initCommitVersion
              : "initInfosVersion=" + initInfosVersion + " initCommitVersion=" + initCommitVersion;
            lock = new ReentrantLock();
        }


        public override string ToString()
        {
            if (isPrimary)
            {
                return "P" + id + " tcpPort=" + tcpPort;
            }
            else
            {
                return "R" + id + " tcpPort=" + tcpPort;
            }
        }

        public void crash()
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                if (isOpen)
                {
                    isOpen = false;
                    p.destroy();
                    try
                    {
                        p.waitFor();
                        pumper.join();
                    }
                    catch (InterruptedException ie)
                    {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(ie);
                    }
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <exception cref="IOException"/>
        /// <returns></returns>
        public bool commit()
        {
            using (Connection c = new Connection(tcpPort))
            {
                c.output.writeByte(SimplePrimaryNode.CMD_COMMIT);
                c.flush();
                c.s.shutdownOutput();
                if (c.in.readByte() != 1) {
                    throw new RuntimeException("commit failed");
                }
                return true;
            }
        }

        /// <exception cref="IOException"/>
        public void commitAsync()
        {
            using (Connection c = new Connection(tcpPort))
            {
                c.output.writeByte(SimplePrimaryNode.CMD_COMMIT);
                c.flush();
            }
        }

        public long getSearchingVersion()
        {
            using (Connection c = new Connection(tcpPort))
            {
                c.output.writeByte(SimplePrimaryNode.CMD_GET_SEARCHING_VERSION);
                c.flush();
                c.s.shutdownOutput();
                return c.input.readVLong();
            }
        }

        /// <summary>
        /// Ask the primary node process to flush. We send it all currently up replicas so it can notify
        /// them about the new NRT point. Returns the newly flushed version, or a negative (current)
        /// version if there were no changes.
        /// </summary>
        /// <exception cref="IOException"/>
        public synchronized long flush(int atLeastMarkerCount)
        {
            assert isPrimary;
            using (Connection c = new Connection(tcpPort))
            {
                c.output.writeByte(SimplePrimaryNode.CMD_FLUSH);
                c.output.writeVInt(atLeastMarkerCount);
                c.flush();
                c.s.shutdownOutput();
                return c.input.readLong();
            }
        }

        public void Dispose()
        {
            shutdown();
        }

        public synchronized boolean shutdown()
        {
            lock.lock ()
                    ;
            try
            {
                // System.out.println("PARENT: now shutdown node=" + id + " isOpen=" + isOpen);
                if (isOpen)
                {
                    // Ask the child process to shutdown gracefully:
                    isOpen = false;
                    // System.out.println("PARENT: send CMD_CLOSE to node=" + id);
                    using (Connection c = new Connection(tcpPort))
                    {
                        c.output.writeByte(SimplePrimaryNode.CMD_CLOSE);
                        c.flush();
                        if (c.input.readByte() != 1)
                        {
                            throw new RuntimeException("shutdown failed");
                        }
                    } catch (Throwable t)
            {
                System.out.println("top: shutdown failed; ignoring");
                t.printStackTrace(System.out);
            }
            try
            {
                p.waitFor();
                pumper.join();
            }
            catch (InterruptedException ie)
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ie);
            }
        }

                return true;
            }
            finally
            {
                lock.unlock();
}
        }

        /// <exception cref="IOException"/>
        public void newNRTPoint(long version, long primaryGen, int primaryTCPPort)
{
    try (Connection c = new Connection(tcpPort)) {
    c.out.writeByte(SimpleReplicaNode.CMD_NEW_NRT_POINT);
    c.out.writeVLong(version);
    c.out.writeVLong(primaryGen);
    c.out.writeInt(primaryTCPPort);
    c.flush();
}
}

// Simulate a replica holding a copy state open forever, by just leaking it.

public void leakCopyState() throws IOException
{
    try (Connection c = new Connection(tcpPort)) {
    c.out.writeByte(SimplePrimaryNode.CMD_LEAK_COPY_STATE);
    c.flush();
}
  }

  public void setRemoteCloseTimeoutMs(int timeoutMs) throws IOException
{
    try (Connection c = new Connection(tcpPort)) {
    c.out.writeByte(SimplePrimaryNode.CMD_SET_CLOSE_WAIT_MS);
    c.out.writeInt(timeoutMs);
    c.flush();
}
  }

  public void addOrUpdateDocument(Connection c, Document doc, boolean isUpdate) throws IOException
{
    if (isPrimary == false) {
        throw new IllegalStateException("only primary can index");
    }
    int fieldCount = 0;

    String title = doc.get("title");
    if (title != null)
    {
        fieldCount++;
    }

    String docid = doc.get("docid");
    assert docid != null;
    fieldCount++;

    String body = doc.get("body");
    if (body != null)
    {
        fieldCount++;
    }

    String marker = doc.get("marker");
    if (marker != null)
    {
        fieldCount++;
    }

    c.out.writeByte(isUpdate ? SimplePrimaryNode.CMD_UPDATE_DOC : SimplePrimaryNode.CMD_ADD_DOC);
    c.out.writeVInt(fieldCount);
    c.out.writeString("docid");
    c.out.writeString(docid);
    if (title != null)
    {
        c.out.writeString("title");
        c.out.writeString(title);
    }
    if (body != null)
    {
        c.out.writeString("body");
        c.out.writeString(body);
    }
    if (marker != null)
    {
        c.out.writeString("marker");
        c.out.writeString(marker);
    }
    c.flush();
    c.in.readByte();
    }

  public void deleteDocument(Connection c, String docid) throws IOException
{
    if (isPrimary == false) {
        throw new IllegalStateException("only primary can delete documents");
    }
    c.out.writeByte(SimplePrimaryNode.CMD_DELETE_DOC);
    c.out.writeString(docid);
    c.flush();
    c.in.readByte();
}

public void deleteAllDocuments(Connection c) throws IOException
{
    if (isPrimary == false) {
        throw new IllegalStateException("only primary can delete documents");
    }
    c.out.writeByte(SimplePrimaryNode.CMD_DELETE_ALL_DOCS);
    c.flush();
    c.in.readByte();
}

public void forceMerge(Connection c) throws IOException
{
    if (isPrimary == false) {
        throw new IllegalStateException("only primary can force merge");
    }
    c.out.writeByte(SimplePrimaryNode.CMD_FORCE_MERGE);
    c.flush();
    c.in.readByte();
}
}
}
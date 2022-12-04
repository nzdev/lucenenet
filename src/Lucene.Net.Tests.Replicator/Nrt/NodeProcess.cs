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
            bool isPrimary,
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
                if (c.input.readByte() != 1)
                {
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
        public long flush(int atLeastMarkerCount)
        {
            UninterruptableMonitor.Enter(this);
            try
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
            finally
            {
                UninterruptableMonitor.Exit(this);
            }

        }

        public void Dispose()
        {
            shutdown();
        }

        //TODO: REDO
        public bool shutdown()
        {
            UninterruptableMonitor.Enter(this);
            try
            {
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

                return true;
            }


            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }

        /// <exception cref="IOException"/>
        public void newNRTPoint(long version, long primaryGen, int primaryTCPPort)
        {
            using (Connection c = new Connection(tcpPort))
            {
                c.output.writeByte(SimpleReplicaNode.CMD_NEW_NRT_POINT);
                c.output.writeVLong(version);
                c.output.writeVLong(primaryGen);
                c.output.writeInt(primaryTCPPort);
                c.flush();
            }
        }

        /// <summary>
        /// Simulate a replica holding a copy state open forever, by just leaking it.
        /// </summary>
        /// <exception cref="IOException"/>
        public void leakCopyState()
        {
            using (Connection c = new Connection(tcpPort))
            {
                c.output.writeByte(SimplePrimaryNode.CMD_LEAK_COPY_STATE);
                c.flush();
            }
        }

        /// <exception cref="IOException"/>
        public void setRemoteCloseTimeoutMs(int timeoutMs)
        {
            using (Connection c = new Connection(tcpPort))
            {
                c.output.writeByte(SimplePrimaryNode.CMD_SET_CLOSE_WAIT_MS);
                c.output.writeInt(timeoutMs);
                c.flush();
            }
        }

        /// <exception cref="IOException"/>
        public void addOrUpdateDocument(Connection c, Document doc, boolean isUpdate)
        {
            if (isPrimary == false)
            {
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

            c.output.writeByte(isUpdate ? SimplePrimaryNode.CMD_UPDATE_DOC : SimplePrimaryNode.CMD_ADD_DOC);
            c.output.writeVInt(fieldCount);
            c.output.writeString("docid");
            c.output.writeString(docid);
            if (title != null)
            {
                c.output.writeString("title");
                c.output.writeString(title);
            }
            if (body != null)
            {
                c.output.writeString("body");
                c.output.writeString(body);
            }
            if (marker != null)
            {
                c.output.writeString("marker");
                c.output.writeString(marker);
            }
            c.flush();
            c.input.readByte();
        }
        /// <exception cref="IOException"/>
        public void deleteDocument(Connection c, String docid)
        {
            if (isPrimary == false)
            {
                throw new IllegalStateException("only primary can delete documents");
            }
            c.output.writeByte(SimplePrimaryNode.CMD_DELETE_DOC);
            c.output.writeString(docid);
            c.flush();
            c.input.readByte();
        }
        /// <exception cref="IOException"/>
        public void deleteAllDocuments(Connection c)
        {
            if (isPrimary == false)
            {
                throw new IllegalStateException("only primary can delete documents");
            }
            c.output.writeByte(SimplePrimaryNode.CMD_DELETE_ALL_DOCS);
            c.flush();
            c.input.readByte();
        }
        /// <exception cref="IOException"/>
        public void forceMerge(Connection c)
        {
            if (isPrimary == false)
            {
                throw new IllegalStateException("only primary can force merge");
            }
            c.output.writeByte(SimplePrimaryNode.CMD_FORCE_MERGE);
            c.flush();
            c.input.readByte();
        }
    }
}

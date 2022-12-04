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

using Lucene.Net.Store;
using System;
using System.Net.Sockets;
//import java.io.BufferedOutputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.net.InetAddress;
//import java.net.Socket;
namespace Lucene.Net.Replicator.Nrt
{
    /// <summary>
    /// Simple point-to-point TCP connection
    /// </summary>
    class Connection : IDisposable
    {
        public readonly DataInput input;
        public readonly DataOutput output;
        public readonly InputStream sockIn;
        public readonly BufferedOutputStream bos;
        public readonly Socket s;
        public readonly int destTCPPort;
        public long lastKeepAliveNS = System.nanoTime();

        /// <summary>
        /// 
        /// </summary>
        /// <exception cref="IOException"/>
        /// <param name="tcpPort"></param>
        public Connection(int tcpPort)
        {
            this.destTCPPort = tcpPort;
            this.s = new Socket(InetAddress.getLoopbackAddress(), tcpPort);
            this.sockIn = s.getInputStream();
            this.input = new InputStreamDataInput(sockIn);
            this.bos = new BufferedOutputStream(s.getOutputStream());
            this.output = new OutputStreamDataOutput(bos);
            if (Node.VERBOSE_CONNECTIONS)
            {
                System.out.println("make new client Connection socket=" + this.s + " destPort=" + tcpPort);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <exception cref="IOException"
        public void flush()
        {
            bos.flush();
        }

        /// <summary>
        /// Close
        /// </summary>
        /// <exception cref="IOException"
        public void Dispose()
        {
            s.close();
        }

    }
}
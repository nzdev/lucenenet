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

//import java.io.Closeable;
//import java.io.IOException;
//import java.util.PriorityQueue;
//import org.apache.lucene.store.AlreadyClosedException;
using DotLiquid.Exceptions;
using Lucene.Net.Diagnostics;
using Lucene.Net.Support.Threading;
using Lucene.Net.Util;
using System;
using System.Threading;

namespace Lucene.Net.Replicator.Nrt
{




    /**
     * Runs CopyJob(s) in background thread; each ReplicaNode has an instance of this running. At a
     * given there could be one NRT copy job running, and multiple pre-warm merged segments jobs.
     */
    class Jobs : Thread, IDisposable
    {

        private readonly PriorityQueue<CopyJob> queue = new PriorityQueue<>();

        private readonly Node node;

        public Jobs(Node node)
        {
            this.node = node;
        }

        private bool finish;

        /**
         * Returns null if we are closing, else, returns the top job or waits for one to arrive if the
         * queue is empty.
         */
        private SimpleCopyJob GetNextJob()
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                while (true)
                {
                    if (finish)
                    {
                        return null;
                    }
                    else if (queue.IsEmpty())
                    {
                        try
                        {
                            Wait();
                        }
                        catch (InterruptedException ie)
                        {
                            throw new RuntimeException(ie);
                        }
                    }
                    else
                    {
                        return (SimpleCopyJob)queue.poll();
                    }
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }

      public override void Run()
        {
            while (true)
            {
                SimpleCopyJob topJob = GetNextJob();
                if (topJob == null)
                {
                    if (Debugging.AssertsEnabled)
                    {
                        Debugging.Assert(finish);
                    }
                    break;
                }

                this.SetName("jobs o" + topJob.ord);
                if (Debugging.AssertsEnabled)
                {
                    Debugging.Assert(topJob != null);
                }

                boolean result;
                try
                {
                    result = topJob.Visit();
                }
                catch (Exception t)
                {
                    if ((t is AlreadyClosedException) == false)
                    {
                        node.Message("exception during job.visit job=" + topJob + "; now cancel");
                        t.printStackTrace(System.out);
                    }
                    else
                    {
                        node.Message("AlreadyClosedException during job.visit job=" + topJob + "; now cancel");
                    }
                    try
                    {
                        topJob.Csancel("unexpected exception in visit", t);
                    }
                    catch (Exception t2)
                    {
                        node.Message("ignore exception calling cancel: " + t2);
                        t2.printStackTrace(System.out);
                    }
                    try
                    {
                        topJob.onceDone.run(topJob);
                    }
                    catch (Exception t2)
                    {
                        node.Message("ignore exception calling OnceDone: " + t2);
                        t2.printStackTrace(System.out);
                    }
                    continue;
                }

                if (result == false)
                {
                    // Job isn't done yet; put it back:
                    synchronized(this) {
                        queue.offer(topJob);
                    }
                }
                else
                {
                    // Job finished, now notify caller:
                    try
                    {
                        topJob.onceDone.run(topJob);
                    }
                    catch (Exception t)
                    {
                        node.Message("ignore exception calling OnceDone: " + t);
                        t.printStackTrace(System.out);
                    }
                }
            }

            node.Message("top: jobs now exit run thread");

            synchronized(this) {
                // Gracefully cancel any jobs we didn't finish:
                while (queue.IsEmpty() == false)
                {
                    SimpleCopyJob job = (SimpleCopyJob)queue.poll();
                    node.Message("top: Jobs: now cancel job=" + job);
                    try
                    {
                        job.Cancel("jobs closing", null);
                    }
                    catch (Exception t)
                    {
                        node.Message("ignore exception calling cancel");
                        t.printStackTrace(System.out);
                    }
                    try
                    {
                        job.onceDone.run(job);
                    }
                    catch (Exception t)
                    {
                        node.Message("ignore exception calling OnceDone");
                        t.printStackTrace(System.out);
                    }
                }
            }
        }

        public void Launch(CopyJob job)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                if (finish == false)
                {
                    queue.offer(job);
                    Notify();
                }
                else
                {
                    throw new AlreadyClosedException("closed");
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
            
        }

        /** Cancels any existing jobs that are copying the same file names as this one */
        /// <exception cref="IOException"/>
        public void CancelConflictingJobs(CopyJob newJob)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                foreach (CopyJob job in queue)
                {
                    if (job.Conflicts(newJob))
                    {
                        node.Message(
                            "top: now cancel existing conflicting job=" + job + " due to newJob=" + newJob);
                        job.Cancel("conflicts with new job", null);
                    }
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
            
        }

      public override void Close()
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                finish = true;
                Notify();
                try
                {
                    Join();
                }
                catch (InterruptedException ie)
                {
                    throw new RuntimeException(ie);
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
            
        }
    }
}
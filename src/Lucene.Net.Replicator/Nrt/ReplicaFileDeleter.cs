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

using Lucene.Net.Index;
using Lucene.Net.Store;
using System.Collections.Generic;
using System;
using Lucene.Net.Support.Threading;
using Lucene.Net.Diagnostics;
using System.IO;
using JCG = J2N.Collections.Generic;
using Directory = Lucene.Net.Store.Directory;
namespace Lucene.Net.Replicator.Nrt
{



    // TODO: can we factor/share with IFD: this is doing exactly the same thing, but on the replica side

    class ReplicaFileDeleter
    {
        private readonly IDictionary<string, int?> refCounts = new Dictionary<string, int?>();
        private readonly Directory dir;
        private readonly Node node;

        /// <exception cref="IOException"/>
        public ReplicaFileDeleter(Node node, Directory dir)
        {
            this.dir = dir;
            this.node = node;
        }

        /**
         * Used only by asserts: returns true if the file exists (can be opened), false if it cannot be
         * opened, and (unlike Java's File.exists) throws IOException if there's some unexpected error.
         */
        /// <exception cref="IOException"/>
        private static bool slowFileExists(Directory dir, string fileName)
        {
            try
            {
                dir.OpenInput(fileName, IOContext.DEFAULT).Close();
                return true;
            }
            //@SuppressWarnings("unused")
            catch (Exception e) when (e is NoSuchFileException || e is FileNotFoundException)
            {
                return false;
            }
        }

        /// <exception cref="IOException"/>
        public void incRef(ICollection<string> fileNames)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                foreach (string fileName in fileNames)
                {
                    if (Debugging.AssertsEnabled)
                    {
                        Debugging.Assert(slowFileExists(dir, fileName), "file " + fileName + " does not exist!");
                    }

                    int curCount = refCounts.get(fileName);
                    if (curCount == null)
                    {
                        refCounts.put(fileName, 1);
                    }
                    else
                    {
                        refCounts.put(fileName, curCount.intValue() + 1);
                    }
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }

        }

        /// <exception cref="IOException"/>
        public void decRef(ICollection<string> fileNames)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                ISet<string> toDelete = new JCG.HashSet<string>();
                foreach (string fileName in fileNames)
                {
                    int? curCount = refCounts[fileName];
                    if (Debugging.AssertsEnabled)
                    {
                        Debugging.Assert(curCount != null, "fileName=" + fileName);
                        Debugging.Assert(curCount.Value > 0);
                    }
                    
                    if (curCount.Value == 1)
                    {
                        refCounts.Remove(fileName);
                        toDelete.Add(fileName);
                    }
                    else
                    {
                        refCounts.put(fileName, curCount.intValue() - 1);
                    }
                }

                Delete(toDelete);

                // TODO: this local IR could incRef files here, like we do now with IW's NRT readers ... then we
                // can assert this again:

                // we can't assert this, e.g a search can be running when we switch to a new NRT point, holding
                // a previous IndexReader still open for
                // a bit:
                /*
                // We should never attempt deletion of a still-open file:
                Set<String> delOpen = ((MockDirectoryWrapper) dir).getOpenDeletedFiles();
                if (delOpen.isEmpty() == false) {
                  node.message("fail: we tried to delete these still-open files: " + delOpen);
                  throw new AssertionError("we tried to delete these still-open files: " + delOpen);
                }
                */
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }

        }

        /// <exception cref="IOException"/>
        private void Delete(ICollection<String> toDelete)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                if (Node.VERBOSE_FILES)
                {
                    node.message("now delete " + toDelete.Count + " files: " + toDelete);
                }

                // First pass: delete any segments_N files.  We do these first to be certain stale commit points
                // are removed
                // before we remove any files they reference, in case we crash right now:
                foreach (string fileName in toDelete)
                {
                    if (Debugging.AssertsEnabled)
                    {
                        Debugging.Assert(refCounts.ContainsKey(fileName) == false);
                    }

                    if (fileName.StartsWith(IndexFileNames.SEGMENTS))
                    {
                        delete(fileName);
                    }
                }

                // Only delete other files if we were able to remove the segments_N files; this way we never
                // leave a corrupt commit in the index even in the presense of virus checkers:
                foreach (string fileName in toDelete)
                {
                    if (Debugging.AssertsEnabled)
                    {
                        Debugging.Assert(refCounts.ContainsKey(fileName) == false);
                    }

                    if (fileName.StartsWith(IndexFileNames.SEGMENTS) == false)
                    {
                        delete(fileName);
                    }
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }

        }

        /// <exception cref="IOException"/>
        private void delete(string fileName)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                if (Node.VERBOSE_FILES)
                {
                    node.message("file " + fileName + ": now delete");
                }
                dir.DeleteFile(fileName);
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }

        public int GetRefCount(string fileName)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                if (refCounts.ContainsKey(fileName) == false)
                {
                    return refCounts[fileName];
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }

        /// <exception cref="IOException"/>
        public void DeleteIfNoRef(string fileName)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                if (refCounts.ContainsKey(fileName) == false)
                {
                    DeleteNewFile(fileName);
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }

        /// <exception cref="IOException"/>
        public void DeleteNewFile(string fileName)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                delete(fileName);
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }

        /*
        public synchronized Set<String> getPending() {
          return new HashSet<String>(pending);
        }
        */

        /// <exception cref="IOException"/>
        public void DeleteUnknownFiles(string segmentsFileName)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                ISet<String> toDelete = new JCG.HashSet<string>();
                foreach (string fileName in dir.ListAll())
                {
                    if (refCounts.ContainsKey(fileName) == false
                        && fileName.Equals("write.lock") == false
                        && fileName.Equals(segmentsFileName) == false)
                    {
                        node.message("will delete unknown file \"" + fileName + "\"");
                        toDelete.Add(fileName);
                    }
                }

                Delete(toDelete);
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }
    }
}
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

using Lucene.Net.Index;
using Lucene.Net.Store;
using System.Collections.Generic;
using System;
using Lucene.Net.Support.Threading;
using J2N.Text;

namespace Lucene.Net.Replicator.Nrt
{

    import java.io.FileNotFoundException;
    import java.io.IOException;
    import java.nio.file.NoSuchFileException;
    import java.util.Collection;
    import java.util.HashMap;
    import java.util.HashSet;
    import java.util.Map;
    import java.util.Set;

    // TODO: can we factor/share with IFD: this is doing exactly the same thing, but on the replica side

    class ReplicaFileDeleter
    {
        private readonly Map<String, Integer> refCounts = new HashMap<String, Integer>();
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
                dir.openInput(fileName, IOContext.DEFAULT).close();
                return true;
            }
            catch (@SuppressWarnings("unused") NoSuchFileException | FileNotFoundException e) {
                return false;
            }
        }

        /// <exception cref="IOException"/>
        public void incRef(Collection<String> fileNames)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                foreach (string fileName in fileNames)
                {

                    assert slowFileExists(dir, fileName) : "file " + fileName + " does not exist!";

                    Integer curCount = refCounts.get(fileName);
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
        public void decRef(Collection<String> fileNames)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                Set<String> toDelete = new HashSet<>();
                foreach (string fileName in fileNames)
                {
                    Integer curCount = refCounts.get(fileName);
                    assert curCount != null : "fileName=" + fileName;
                    assert curCount.intValue() > 0;
                    if (curCount.intValue() == 1)
                    {
                        refCounts.remove(fileName);
                        toDelete.add(fileName);
                    }
                    else
                    {
                        refCounts.put(fileName, curCount.intValue() - 1);
                    }
                }

                delete(toDelete);

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
        private void delete(Collection<String> toDelete)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                if (Node.VERBOSE_FILES)
                {
                    node.message("now delete " + toDelete.size() + " files: " + toDelete);
                }

                // First pass: delete any segments_N files.  We do these first to be certain stale commit points
                // are removed
                // before we remove any files they reference, in case we crash right now:
                foreach (string fileName in toDelete)
                {
                    assert refCounts.containsKey(fileName) == false;
                    if (fileName.startsWith(IndexFileNames.SEGMENTS))
                    {
                        delete(fileName);
                    }
                }

                // Only delete other files if we were able to remove the segments_N files; this way we never
                // leave a corrupt commit in the index even in the presense of virus checkers:
                foreach (string fileName in toDelete)
                {
                    assert refCounts.containsKey(fileName) == false;
                    if (fileName.startsWith(IndexFileNames.SEGMENTS) == false)
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
                dir.deleteFile(fileName);
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }

        public int getRefCount(string fileName)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                if (refCounts.containsKey(fileName) == false)
                {
                    return refCounts.get(fileName);
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }

        /// <exception cref="IOException"/>
        public void deleteIfNoRef(string fileName)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                if (refCounts.containsKey(fileName) == false)
                {
                    deleteNewFile(fileName);
                }
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }

        /// <exception cref="IOException"/>
        public void deleteNewFile(string fileName)
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
        public void deleteUnknownFiles(string segmentsFileName)
        {
            UninterruptableMonitor.Enter(this);
            try
            {
                Set<String> toDelete = new HashSet<>();
                foreach (string fileName in dir.listAll())
                {
                    if (refCounts.containsKey(fileName) == false
                        && fileName.Equals("write.lock") == false
                        && fileName.Equals(segmentsFileName) == false)
                    {
                        node.message("will delete unknown file \"" + fileName + "\"");
                        toDelete.add(fileName);
                    }
                }

                delete(toDelete);
            }
            finally
            {
                UninterruptableMonitor.Exit(this);
            }
        }
    }
}
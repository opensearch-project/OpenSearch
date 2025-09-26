/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.lucene.store.FilterIndexOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.SingleObjectCache;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.util.Set;

/**
 * Caching directory sized in bytes
 *
 * @opensearch.internal
 */
final class ByteSizeCachingDirectory extends FilterDirectory {

    /**
     * Internal caching size and modulo count
     *
     * @opensearch.internal
     */
    private static class SizeAndModCount {
        final long size;
        final long modCount;
        final boolean pendingWrite;

        SizeAndModCount(long length, long modCount, boolean pendingWrite) {
            this.size = length;
            this.modCount = modCount;
            this.pendingWrite = pendingWrite;
        }
    }

    private static long estimateSizeInBytes(Directory directory) throws IOException {
        long estimatedSize = 0;
        String[] files = directory.listAll();
        for (String file : files) {
            try {
                estimatedSize += directory.fileLength(file);
            } catch (NoSuchFileException | FileNotFoundException | AccessDeniedException e) {
                // ignore, the file is not there no more; on Windows, if one thread concurrently deletes a file while
                // calling Files.size, you can also sometimes hit AccessDeniedException
            }
        }
        return estimatedSize;
    }

    private final SingleObjectCache<SizeAndModCount> size;
    // Both these variables need to be accessed under `this` lock.
    private long modCount = 0;
    private long numOpenOutputs = 0;

    ByteSizeCachingDirectory(Directory in, TimeValue refreshInterval) {
        super(in);
        size = new SingleObjectCache<SizeAndModCount>(refreshInterval, new SizeAndModCount(0L, -1L, true)) {
            @Override
            protected SizeAndModCount refresh() {
                // It is ok for the size of the directory to be more recent than
                // the mod count, we would just recompute the size of the
                // directory on the next call as well. However the opposite
                // would be bad as we would potentially have a stale cache
                // entry for a long time. So we fetch the values of modCount and
                // numOpenOutputs BEFORE computing the size of the directory.
                final long modCount;
                final boolean pendingWrite;
                synchronized (ByteSizeCachingDirectory.this) {
                    modCount = ByteSizeCachingDirectory.this.modCount;
                    pendingWrite = ByteSizeCachingDirectory.this.numOpenOutputs != 0;
                }
                final long size;
                try {
                    // Compute this OUTSIDE of the lock
                    size = estimateSizeInBytes(getDelegate());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return new SizeAndModCount(size, modCount, pendingWrite);
            }

            @Override
            protected boolean needsRefresh() {
                if (super.needsRefresh() == false) {
                    // The size was computed recently, don't recompute
                    return false;
                }
                SizeAndModCount cached = getNoRefresh();
                if (cached.pendingWrite) {
                    // The cached entry was generated while there were pending
                    // writes, so the size might be stale: recompute.
                    return true;
                }
                synchronized (ByteSizeCachingDirectory.this) {
                    // If there are pending writes or if new files have been
                    // written/deleted since last time: recompute
                    return numOpenOutputs != 0 || cached.modCount != modCount;
                }
            }
        };
    }

    /** Return the cumulative size of all files in this directory. */
    long estimateSizeInBytes() throws IOException {
        try {
            return size.getOrRefresh().size;
        } catch (UncheckedIOException e) {
            // we wrapped in the cache and unwrap here
            throw e.getCause();
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return wrapIndexOutput(super.createOutput(name, context));
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return wrapIndexOutput(super.createTempOutput(prefix, suffix, context));
    }

    private IndexOutput wrapIndexOutput(IndexOutput out) {
        synchronized (this) {
            numOpenOutputs++;
        }
        return new FilterIndexOutput(out.toString(), out) {
            @Override
            public void writeBytes(byte[] b, int length) throws IOException {
                // Don't write to atomicXXX here since it might be called in
                // tight loops and memory barriers are costly
                super.writeBytes(b, length);
            }

            @Override
            public void writeByte(byte b) throws IOException {
                // Don't write to atomicXXX here since it might be called in
                // tight loops and memory barriers are costly
                super.writeByte(b);
            }

            @Override
            public void writeInt(int i) throws IOException {
                out.writeInt(i);
            }

            @Override
            public void writeShort(short i) throws IOException {
                out.writeShort(i);
            }

            @Override
            public void writeLong(long i) throws IOException {
                out.writeLong(i);
            }

            @Override
            public void close() throws IOException {
                // Close might cause some data to be flushed from in-memory buffers, so
                // increment the modification counter too.
                try {
                    super.close();
                } finally {
                    synchronized (ByteSizeCachingDirectory.this) {
                        numOpenOutputs--;
                        modCount++;
                    }
                }
            }
        };
    }

    @Override
    public void deleteFile(String name) throws IOException {
        try {
            super.deleteFile(name);
        } finally {
            synchronized (this) {
                modCount++;
            }
        }
    }

    // temporary override until LUCENE-8735 is integrated
    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return in.getPendingDeletions();
    }
}

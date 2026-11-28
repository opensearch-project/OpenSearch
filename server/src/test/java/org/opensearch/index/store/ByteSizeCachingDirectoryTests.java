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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.remote.directory.RemoteSnapshotDirectory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class ByteSizeCachingDirectoryTests extends OpenSearchTestCase {

    private static class LengthCountingDirectory extends FilterDirectory {

        int numFileLengthCalls;

        LengthCountingDirectory(Directory in) {
            super(in);
        }

        @Override
        public long fileLength(String name) throws IOException {
            numFileLengthCalls++;
            return super.fileLength(name);
        }

        // temporary override until LUCENE-8735 is integrated
        @Override
        public Set<String> getPendingDeletions() throws IOException {
            return in.getPendingDeletions();
        }
    }

    public void testBasics() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("quux", IOContext.DEFAULT)) {
                out.writeBytes(new byte[11], 11);
            }
            LengthCountingDirectory countingDir = new LengthCountingDirectory(dir);

            ByteSizeCachingDirectory cachingDir = new ByteSizeCachingDirectory(countingDir, new TimeValue(0));
            assertEquals(11, cachingDir.estimateSizeInBytes());
            assertEquals(11, cachingDir.estimateSizeInBytes());
            assertEquals(1, countingDir.numFileLengthCalls);

            try (IndexOutput out = cachingDir.createOutput("foo", IOContext.DEFAULT)) {
                out.writeBytes(new byte[5], 5);

                cachingDir.estimateSizeInBytes();
                // +2 because there are 3 files
                assertEquals(3, countingDir.numFileLengthCalls);
                // An index output is open so no caching
                cachingDir.estimateSizeInBytes();
                assertEquals(5, countingDir.numFileLengthCalls);
            }

            assertEquals(16, cachingDir.estimateSizeInBytes());
            assertEquals(7, countingDir.numFileLengthCalls);
            assertEquals(16, cachingDir.estimateSizeInBytes());
            assertEquals(7, countingDir.numFileLengthCalls);

            try (IndexOutput out = cachingDir.createTempOutput("bar", "baz", IOContext.DEFAULT)) {
                out.writeBytes(new byte[4], 4);

                cachingDir.estimateSizeInBytes();
                assertEquals(10, countingDir.numFileLengthCalls);
                // An index output is open so no caching
                cachingDir.estimateSizeInBytes();
                assertEquals(13, countingDir.numFileLengthCalls);
            }

            assertEquals(20, cachingDir.estimateSizeInBytes());
            // +3 because there are 3 files
            assertEquals(16, countingDir.numFileLengthCalls);
            assertEquals(20, cachingDir.estimateSizeInBytes());
            assertEquals(16, countingDir.numFileLengthCalls);

            cachingDir.deleteFile("foo");

            assertEquals(15, cachingDir.estimateSizeInBytes());
            // +2 because there are 2 files now
            assertEquals(18, countingDir.numFileLengthCalls);
            assertEquals(15, cachingDir.estimateSizeInBytes());
            assertEquals(18, countingDir.numFileLengthCalls);
        }
    }

    public void testSnapshotStatsDoNotLoadOrWaitForMetadata() throws Exception {
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger reads = new AtomicInteger();
        BlobStoreIndexShardSnapshot snapshot = mock(BlobStoreIndexShardSnapshot.class);
        BlobStoreIndexShardSnapshot.FileInfo file = new BlobStoreIndexShardSnapshot.FileInfo(
            "__0",
            new StoreFileMetadata("_0.cfs", 123, "", org.apache.lucene.util.Version.LATEST),
            null
        );
        when(snapshot.indexFiles()).thenAnswer(invocation -> {
            reads.incrementAndGet();
            loading.countDown();
            assertTrue(release.await(30, TimeUnit.SECONDS));
            return Collections.singletonList(file);
        });
        try (RemoteSnapshotDirectory remote = new RemoteSnapshotDirectory(snapshot, FSDirectory.open(createTempDir()), null)) {
            // Store can wrap the remote directory in other FilterDirectories.
            ByteSizeCachingDirectory directory = new ByteSizeCachingDirectory(
                new LengthCountingDirectory(remote),
                TimeValue.timeValueHours(1)
            );
            FutureTask<String[]> load = new FutureTask<>(remote::listAll);
            Thread loader = new Thread(load);
            FutureTask<Long> before = new FutureTask<>(directory::estimateSizeInBytes);
            Thread statsBefore = new Thread(before);
            FutureTask<Long> during = new FutureTask<>(directory::estimateSizeInBytes);
            Thread statsDuring = new Thread(during);
            try {
                statsBefore.start();
                assertEquals(0L, before.get(10, TimeUnit.SECONDS).longValue());
                assertEquals(0, reads.get());
                loader.start();
                assertTrue(loading.await(10, TimeUnit.SECONDS));
                statsDuring.start();
                assertEquals(0L, during.get(10, TimeUnit.SECONDS).longValue());
                assertEquals(1, reads.get());
                release.countDown();
                assertArrayEquals(new String[] { "_0.cfs" }, load.get(10, TimeUnit.SECONDS));
                // The temporary zero must not survive in the size cache after loading.
                assertEquals(123L, directory.estimateSizeInBytes());
                assertEquals(123L, directory.estimateSizeInBytes());
                assertEquals(1, reads.get());
            } finally {
                release.countDown();
                loader.join();
                statsBefore.join();
                statsDuring.join();
            }
        }
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public abstract class TransferManagerTestCase extends OpenSearchTestCase {
    protected static final int EIGHT_MB = 1024 * 1024 * 8;
    protected final FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(
        EIGHT_MB * 2,
        1,
        new NoopCircuitBreaker(CircuitBreaker.REQUEST)
    );
    protected MMapDirectory directory;
    protected TransferManager transferManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        directory = new MMapDirectory(createTempDir(), SimpleFSLockFactory.INSTANCE);
        initializeTransferManager();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    protected static byte[] createData() {
        final byte[] data = new byte[EIGHT_MB];
        data[EIGHT_MB - 1] = 7;
        return data;
    }

    public void testSingleAccess() throws Exception {
        try (IndexInput i = fetchBlobWithName("file")) {
            assertIndexInputIsFunctional(i);
            MatcherAssert.assertThat(fileCache.usage().activeUsage(), equalTo((long) EIGHT_MB));
        }
        MatcherAssert.assertThat(fileCache.usage().activeUsage(), equalTo(0L));
        MatcherAssert.assertThat(fileCache.usage().usage(), equalTo((long) EIGHT_MB));
    }

    public void testConcurrentAccess() throws Exception {
        // Kick off multiple threads that all concurrently request the same resource
        final String blobname = "file";
        final ExecutorService testRunner = Executors.newFixedThreadPool(8);
        try {
            final List<Future<IndexInput>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                futures.add(testRunner.submit(() -> fetchBlobWithName(blobname)));
            }
            // Wait for all threads to complete
            for (Future<IndexInput> future : futures) {
                future.get(1, TimeUnit.SECONDS);
            }
            // Assert that all IndexInputs are independently positioned by seeking
            // to the end and closing each one. If not independent, then this would
            // result in EOFExceptions and/or NPEs.
            for (Future<IndexInput> future : futures) {
                try (IndexInput i = future.get()) {
                    assertIndexInputIsFunctional(i);
                }
            }
        } finally {
            assertTrue(terminate(testRunner));
        }
    }

    public void testFetchBlobWithConcurrentCacheEvictions() throws Exception {
        // Submit 256 tasks to an executor with 16 threads that will each randomly
        // request one of eight blobs. Given that the cache can only hold two
        // blobs this will lead to a huge amount of contention and thrashing.
        final ExecutorService testRunner = Executors.newFixedThreadPool(16);
        try {
            final List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 256; i++) {
                // request an index input and immediately close it
                final String blobname = "blob-" + randomIntBetween(0, 7);
                futures.add(testRunner.submit(() -> {
                    try {
                        try (IndexInput indexInput = fetchBlobWithName(blobname)) {
                            assertIndexInputIsFunctional(indexInput);
                        }
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }));
            }
            // Wait for all threads to complete
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        } finally {
            assertTrue(terminate(testRunner));
        }
        MatcherAssert.assertThat("Expected many evictions to happen", fileCache.stats().evictionCount(), greaterThan(0L));
    }

    public void testUsageExceedsCapacity() throws Exception {
        // Fetch resources that exceed the configured capacity of the cache and assert that the
        // returned IndexInputs are still functional.
        try (IndexInput i1 = fetchBlobWithName("1"); IndexInput i2 = fetchBlobWithName("2"); IndexInput i3 = fetchBlobWithName("3")) {
            assertIndexInputIsFunctional(i1);
            assertIndexInputIsFunctional(i2);
            assertIndexInputIsFunctional(i3);
            MatcherAssert.assertThat(fileCache.usage().activeUsage(), equalTo((long) EIGHT_MB * 3));
            MatcherAssert.assertThat(fileCache.usage().usage(), equalTo((long) EIGHT_MB * 3));
        }
        MatcherAssert.assertThat(fileCache.usage().activeUsage(), equalTo(0L));
        MatcherAssert.assertThat(fileCache.usage().usage(), equalTo((long) EIGHT_MB * 3));
        // Fetch another resource which will trigger an eviction
        try (IndexInput i1 = fetchBlobWithName("1")) {
            assertIndexInputIsFunctional(i1);
            MatcherAssert.assertThat(fileCache.usage().activeUsage(), equalTo((long) EIGHT_MB));
            MatcherAssert.assertThat(fileCache.usage().usage(), equalTo((long) EIGHT_MB));
        }
        MatcherAssert.assertThat(fileCache.usage().activeUsage(), equalTo(0L));
        MatcherAssert.assertThat(fileCache.usage().usage(), equalTo((long) EIGHT_MB));
    }

    public void testDownloadFails() throws Exception {
        mockExceptionWhileReading();
        List<BlobFetchRequest.BlobPart> blobParts = new ArrayList<>();
        blobParts.add(new BlobFetchRequest.BlobPart("failure-blob", 0, EIGHT_MB));
        expectThrows(
            IOException.class,
            () -> transferManager.fetchBlob(BlobFetchRequest.builder().fileName("file").directory(directory).blobParts(blobParts).build())
        );
        MatcherAssert.assertThat(fileCache.usage().activeUsage(), equalTo(0L));
        MatcherAssert.assertThat(fileCache.usage().usage(), equalTo(0L));
    }

    public void testFetchesToDifferentBlobsDoNotBlockOnEachOther() throws Exception {
        // Mock a call for a blob that will block until the latch is released,
        // then start the fetch for that blob on a separate thread
        final CountDownLatch latch = new CountDownLatch(1);
        mockWaitForLatchReader(latch);
        List<BlobFetchRequest.BlobPart> blobParts = new ArrayList<>();
        blobParts.add(new BlobFetchRequest.BlobPart("blocking-blob", 0, EIGHT_MB));

        final Thread blockingThread = new Thread(() -> {
            try {
                transferManager.fetchBlob(
                    BlobFetchRequest.builder().fileName("blocking-file").directory(directory).blobParts(blobParts).build()
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        blockingThread.start();

        // Assert that a different blob can be fetched and will not block on the first blob
        try (IndexInput i = fetchBlobWithName("file")) {
            assertIndexInputIsFunctional(i);
        }

        assertTrue(blockingThread.isAlive());
        latch.countDown();
        blockingThread.join(5_000);
        assertFalse(blockingThread.isAlive());
    }

    protected abstract void initializeTransferManager() throws IOException;

    protected abstract void mockExceptionWhileReading() throws IOException;

    protected abstract void mockWaitForLatchReader(CountDownLatch latch) throws IOException;

    private IndexInput fetchBlobWithName(String blobname) throws IOException {
        List<BlobFetchRequest.BlobPart> blobParts = new ArrayList<>();
        blobParts.add(new BlobFetchRequest.BlobPart("blob", 0, EIGHT_MB));
        return transferManager.fetchBlob(BlobFetchRequest.builder().fileName(blobname).directory(directory).blobParts(blobParts).build());
    }

    private static void assertIndexInputIsFunctional(IndexInput indexInput) throws IOException {
        indexInput.seek(EIGHT_MB - 1);
        MatcherAssert.assertThat(indexInput.readByte(), equalTo((byte) 7));
    }
}

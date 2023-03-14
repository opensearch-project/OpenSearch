/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class TransferManagerTests extends OpenSearchTestCase {
    private static final int EIGHT_MB = 1024 * 1024 * 8;
    private final FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(EIGHT_MB * 2, 1);
    private MMapDirectory directory;
    private BlobContainer blobContainer;
    private TransferManager transferManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        directory = new MMapDirectory(createTempDir(), SimpleFSLockFactory.INSTANCE);
        blobContainer = mock(BlobContainer.class);
        doAnswer(i -> new ByteArrayInputStream(createData())).when(blobContainer).readBlob(eq("blob"), anyLong(), anyLong());
        transferManager = new TransferManager(blobContainer, fileCache);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private static byte[] createData() {
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
        doThrow(new IOException("Expected test exception")).when(blobContainer).readBlob(eq("failure-blob"), anyLong(), anyLong());
        expectThrows(
            IOException.class,
            () -> transferManager.fetchBlob(
                BlobFetchRequest.builder()
                    .blobName("failure-blob")
                    .position(0)
                    .fileName("file")
                    .directory(directory)
                    .length(EIGHT_MB)
                    .build()
            )
        );
        MatcherAssert.assertThat(fileCache.usage().activeUsage(), equalTo(0L));
        MatcherAssert.assertThat(fileCache.usage().usage(), equalTo(0L));
    }

    private IndexInput fetchBlobWithName(String blobname) throws IOException {
        return transferManager.fetchBlob(
            BlobFetchRequest.builder().blobName("blob").position(0).fileName(blobname).directory(directory).length(EIGHT_MB).build()
        );
    }

    private static void assertIndexInputIsFunctional(IndexInput indexInput) throws IOException {
        indexInput.seek(EIGHT_MB - 1);
        MatcherAssert.assertThat(indexInput.readByte(), equalTo((byte) 7));
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class TransferManagerTests extends OpenSearchTestCase {
    private final FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(1024 * 1024, 8);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private MMapDirectory directory;
    private BlobContainer blobContainer;
    private TransferManager transferManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        directory = new MMapDirectory(createTempDir(), SimpleFSLockFactory.INSTANCE);
        blobContainer = mock(BlobContainer.class);
        doAnswer(i -> new ByteArrayInputStream(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7 })).when(blobContainer).readBlob("blob", 0, 8);
        transferManager = new TransferManager(blobContainer, executor, fileCache);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
    }

    public void testSingleAccess() throws Exception {
        try (IndexInput i = fetchBlob()) {
            i.seek(7);
            MatcherAssert.assertThat(i.readByte(), equalTo((byte) 7));
        }
    }

    public void testConcurrentAccess() throws Exception {
        // Kick off multiple threads that all concurrently request the same resource
        final ExecutorService testRunner = Executors.newFixedThreadPool(8);
        try {
            final List<Future<IndexInput>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                futures.add(testRunner.submit(this::fetchBlob));
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
                    i.seek(7);
                    MatcherAssert.assertThat(i.readByte(), equalTo((byte) 7));
                }
            }
        } finally {
            testRunner.shutdown();
            assertTrue(testRunner.awaitTermination(1, TimeUnit.SECONDS));
        }
    }

    private IndexInput fetchBlob() throws ExecutionException, InterruptedException {
        return transferManager.asyncFetchBlob(
            BlobFetchRequest.builder().blobName("blob").position(0).fileName("file").directory(directory).length(8).build()
        ).get();
    }
}

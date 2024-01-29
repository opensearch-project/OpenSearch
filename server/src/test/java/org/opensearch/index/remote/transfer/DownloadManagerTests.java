/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote.transfer;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.action.ActionListener;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DownloadManagerTests extends OpenSearchTestCase {
    private ThreadPool threadPool;
    private Directory source;
    private Directory destination;
    private Directory secondDestination;
    private BlobContainer blobContainer;
    private DownloadManager downloadManager;
    private Path path;
    private final Map<String, Integer> files = new HashMap<>();

    @Before
    public void setup() throws IOException {
        final int streamLimit = randomIntBetween(1, 20);
        final RecoverySettings recoverySettings = new RecoverySettings(
            Settings.builder().put("indices.recovery.max_concurrent_remote_store_streams", streamLimit).build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        threadPool = new TestThreadPool(getTestName());
        source = new NIOFSDirectory(createTempDir());
        destination = new NIOFSDirectory(createTempDir());
        secondDestination = new NIOFSDirectory(createTempDir());
        blobContainer = mock(BlobContainer.class);
        path = createTempDir("DownloadManagerTests");
        for (int i = 0; i < 10; i++) {
            final String filename = "file_" + i;
            final int content = randomInt();
            try (IndexOutput output = source.createOutput(filename, IOContext.DEFAULT)) {
                output.writeInt(content);
            }
            files.put(filename, content);
        }
        downloadManager = new DownloadManager(threadPool, recoverySettings);
    }

    @After
    public void stopThreadPool() throws Exception {
        threadPool.shutdown();
        assertTrue(threadPool.awaitTermination(5, TimeUnit.SECONDS));
    }

    public void testDownloadFileFromRemoteStoreToMemory() throws Exception {
        byte[] expectedData = "test".getBytes(StandardCharsets.UTF_16);
        when(blobContainer.readBlob(anyString())).thenReturn(new ByteArrayInputStream(expectedData));
        byte[] actualData = downloadManager.downloadFileFromRemoteStoreToMemory(blobContainer, "test-file");
        assertArrayEquals(expectedData, actualData);
    }

    public void testDownloadFileFromRemoteStoreToMemoryException() throws Exception {
        when(blobContainer.readBlob(anyString())).thenThrow(new IOException("something happened"));
        assertThrows(IOException.class, () -> downloadManager.downloadFileFromRemoteStoreToMemory(blobContainer, "test-file"));
    }

    public void testDownloadFileFromRemoteStore() throws Exception {
        final String fileName = "test-file";
        final byte[] expectedData = "test".getBytes(StandardCharsets.UTF_16);
        final Path fileLocation = path.resolve(fileName);
        when(blobContainer.readBlob(anyString())).thenReturn(new ByteArrayInputStream(expectedData));
        downloadManager.downloadFileFromRemoteStore(blobContainer, fileName, path);

        assertTrue(Files.exists(fileLocation));
        assertTrue(Files.size(fileLocation) > 0);
    }

    public void testDownloadFileFromRemoteStoreException() throws Exception {
        final String fileName = "test-file";
        final Path fileLocation = path.resolve(fileName);
        when(blobContainer.readBlob(anyString())).thenThrow(new IOException("unexpected error"));
        assertThrows(IOException.class, () -> downloadManager.downloadFileFromRemoteStore(blobContainer, fileName, path));
        assertFalse(Files.exists(fileLocation));
    }

    public void testCopySegmentsFromRemoteStoreAsync() throws IOException {
        final PlainActionFuture<Void> l = new PlainActionFuture<>();
        downloadManager.copySegmentsFromRemoteStoreAsync(new CancellableThreads(), source, destination, files.keySet(), l);
        l.actionGet();
        assertContent(files, destination);
    }

    public void testCopySegmentsFromRemoteStore() throws IOException, InterruptedException {
        downloadManager.copySegmentsFromRemoteStore(source, destination, secondDestination, files.keySet(), () -> {});
        assertContent(files, destination);
        assertContent(files, secondDestination);
    }

    public void testCopySegmentsWithFileCompletionHandler() throws IOException, InterruptedException {
        final AtomicInteger counter = new AtomicInteger(0);
        downloadManager.copySegmentsFromRemoteStore(source, destination, null, files.keySet(), counter::incrementAndGet);
        assertContent(files, destination);
        assertEquals(files.size(), counter.get());
    }

    public void testCopyNonExistentFile() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        downloadManager.copySegmentsFromRemoteStoreAsync(
            new CancellableThreads(),
            source,
            destination,
            Set.of("not real"),
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {}

                @Override
                public void onFailure(Exception e) {
                    assertEquals(NoSuchFileException.class, e.getClass());
                    latch.countDown();
                }
            }
        );
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    public void testCopySegmentsExtraNonExistentFile() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final List<String> filesWithExtra = new ArrayList<>(files.keySet());
        filesWithExtra.add("not real");
        downloadManager.copySegmentsFromRemoteStoreAsync(
            new CancellableThreads(),
            source,
            destination,
            filesWithExtra,
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {}

                @Override
                public void onFailure(Exception e) {
                    assertEquals(NoSuchFileException.class, e.getClass());
                    latch.countDown();
                }
            }
        );
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    public void testCopySegmentsCancellable() {
        final CancellableThreads cancellableThreads = new CancellableThreads();
        final PlainActionFuture<Void> blockingListener = new PlainActionFuture<>();
        final Directory blockingDestination = new FilterDirectory(destination) {
            @Override
            public void copyFrom(Directory from, String src, String dest, IOContext context) {
                try {
                    Thread.sleep(60_000); // Will be interrupted
                    fail("Expected to be interrupted");
                } catch (InterruptedException e) {
                    throw new RuntimeException("Failed due to interrupt", e);
                }
            }
        };
        downloadManager.copySegmentsFromRemoteStoreAsync(cancellableThreads, source, blockingDestination, files.keySet(), blockingListener);
        assertThrows(
            "Expected to timeout due to blocking directory",
            OpenSearchTimeoutException.class,
            () -> blockingListener.actionGet(TimeValue.timeValueMillis(500))
        );
        cancellableThreads.cancel("test");
        assertThrows(
            "Expected to complete with cancellation failure",
            CancellableThreads.ExecutionCancelledException.class,
            blockingListener::actionGet
        );
    }

    public void testCopySegmentsBlockingCallCanBeInterrupted() throws Exception {
        final Directory blockingDestination = new FilterDirectory(destination) {
            @Override
            public void copyFrom(Directory from, String src, String dest, IOContext context) {
                try {
                    Thread.sleep(60_000); // Will be interrupted
                    fail("Expected to be interrupted");
                } catch (InterruptedException e) {
                    throw new RuntimeException("Failed due to interrupt", e);
                }
            }
        };
        final AtomicReference<Exception> capturedException = new AtomicReference<>();
        final Thread thread = new Thread(() -> {
            try {
                downloadManager.copySegmentsFromRemoteStore(source, blockingDestination, null, files.keySet(), () -> {});
            } catch (Exception e) {
                capturedException.set(e);
            }
        });
        thread.start();
        thread.interrupt();
        thread.join();
        assertEquals(InterruptedException.class, capturedException.get().getClass());
    }

    public void testCopySegmentsIOException() throws IOException, InterruptedException {
        final Directory failureDirectory = new FilterDirectory(destination) {
            @Override
            public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
                throw new IOException("test");
            }
        };
        assertThrows(
            IOException.class,
            () -> downloadManager.copySegmentsFromRemoteStore(source, failureDirectory, null, files.keySet(), () -> {})
        );

        final CountDownLatch latch = new CountDownLatch(1);
        downloadManager.copySegmentsFromRemoteStoreAsync(
            new CancellableThreads(),
            source,
            failureDirectory,
            files.keySet(),
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {}

                @Override
                public void onFailure(Exception e) {
                    assertEquals(IOException.class, e.getClass());
                    latch.countDown();
                }
            }
        );
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    private static void assertContent(Map<String, Integer> expected, Directory destination) throws IOException {
        // Note that Lucene will randomly write extra files (see org.apache.lucene.tests.mockfile.ExtraFS)
        // so we just need to check that all the expected files are present but not that _only_ the expected
        // files are present
        final Set<String> actualFiles = Set.of(destination.listAll());
        for (String file : expected.keySet()) {
            assertTrue(actualFiles.contains(file));
            try (IndexInput input = destination.openInput(file, IOContext.DEFAULT)) {
                assertEquals(expected.get(file), Integer.valueOf(input.readInt()));
                assertThrows(EOFException.class, input::readByte);
            }
        }
    }
}

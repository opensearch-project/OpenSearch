/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;

public class RemoteStoreFileDownloaderTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private Directory source;
    private Directory destination;
    private Directory secondDestination;
    private RemoteStoreFileDownloader fileDownloader;
    private Map<String, Integer> files = new HashMap<>();

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
        for (int i = 0; i < 10; i++) {
            final String filename = "file_" + i;
            final int content = randomInt();
            try (IndexOutput output = source.createOutput(filename, IOContext.DEFAULT)) {
                output.writeInt(content);
            }
            files.put(filename, content);
        }
        fileDownloader = new RemoteStoreFileDownloader(
            ShardId.fromString("[RemoteStoreFileDownloaderTests][0]"),
            threadPool,
            recoverySettings
        );
    }

    @After
    public void stopThreadPool() throws Exception {
        threadPool.shutdown();
        assertTrue(threadPool.awaitTermination(5, TimeUnit.SECONDS));
    }

    public void testDownload() throws IOException {
        final PlainActionFuture<Void> l = new PlainActionFuture<>();
        fileDownloader.downloadAsync(new CancellableThreads(), source, destination, files.keySet(), l);
        l.actionGet();
        assertContent(files, destination);
    }

    public void testDownloadWithSecondDestination() throws IOException, InterruptedException {
        fileDownloader.download(source, destination, secondDestination, files.keySet(), () -> {});
        assertContent(files, destination);
        assertContent(files, secondDestination);
    }

    public void testDownloadWithFileCompletionHandler() throws IOException, InterruptedException {
        final AtomicInteger counter = new AtomicInteger(0);
        fileDownloader.download(source, destination, null, files.keySet(), counter::incrementAndGet);
        assertContent(files, destination);
        assertEquals(files.size(), counter.get());
    }

    public void testDownloadNonExistentFile() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        fileDownloader.downloadAsync(new CancellableThreads(), source, destination, Set.of("not real"), new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {
                assertEquals(NoSuchFileException.class, e.getClass());
                latch.countDown();
            }
        });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    public void testDownloadExtraNonExistentFile() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final List<String> filesWithExtra = new ArrayList<>(files.keySet());
        filesWithExtra.add("not real");
        fileDownloader.downloadAsync(new CancellableThreads(), source, destination, filesWithExtra, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {
                assertEquals(NoSuchFileException.class, e.getClass());
                latch.countDown();
            }
        });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    public void testCancellable() {
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
        fileDownloader.downloadAsync(cancellableThreads, source, blockingDestination, files.keySet(), blockingListener);
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

    public void testBlockingCallCanBeInterrupted() throws Exception {
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
                fileDownloader.download(source, blockingDestination, null, files.keySet(), () -> {});
            } catch (Exception e) {
                capturedException.set(e);
            }
        });
        thread.start();
        thread.interrupt();
        thread.join();
        assertEquals(InterruptedException.class, capturedException.get().getClass());
    }

    public void testIOException() throws IOException, InterruptedException {
        final Directory failureDirectory = new FilterDirectory(destination) {
            @Override
            public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
                throw new IOException("test");
            }
        };
        assertThrows(IOException.class, () -> fileDownloader.download(source, failureDirectory, null, files.keySet(), () -> {}));

        final CountDownLatch latch = new CountDownLatch(1);
        fileDownloader.downloadAsync(new CancellableThreads(), source, failureDirectory, files.keySet(), new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {
                assertEquals(IOException.class, e.getClass());
                latch.countDown();
            }
        });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    /**
     * Files larger than the configured part size that live in a {@link RemoteSegmentStoreDirectory} must be
     * downloaded as multiple concurrent byte-range requests, land byte-identical in the destination, and hand back
     * every prefetch permit afterwards. Files no larger than one part must keep using the single-stream path.
     */
    public void testMultiPartDownloadFromRemoteSegmentStoreDirectory() throws Exception {
        final long partSize = ByteSizeUnit.MB.toBytes(1);
        final int maxParts = randomIntBetween(1, 6);
        final RecoverySettings recoverySettings = new RecoverySettings(
            Settings.builder()
                .put("indices.recovery.max_concurrent_remote_store_streams", randomIntBetween(1, 4))
                .put("indices.recovery.remote_store.parallel_download.part_size", new ByteSizeValue(partSize))
                .put("indices.recovery.remote_store.parallel_download.max_concurrent_parts", maxParts)
                .build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        final RemoteStoreFileDownloader downloader = new RemoteStoreFileDownloader(
            ShardId.fromString("[RemoteStoreFileDownloaderTests][0]"),
            threadPool,
            recoverySettings
        );

        final Path blobPath = createTempDir();
        final RangeCountingBlobContainer blobContainer = new RangeCountingBlobContainer(
            new FsBlobContainer(new FsBlobStore(8 * 1024, blobPath, false), BlobPath.cleanPath(), blobPath)
        );
        final RemoteSegmentStoreDirectory remoteSource = new RemoteSegmentStoreDirectory(
            new RemoteDirectory(blobContainer),
            mock(RemoteDirectory.class),
            mock(RemoteStoreLockManager.class),
            threadPool,
            ShardId.fromString("[RemoteStoreFileDownloaderTests][0]")
        );

        // Upload a mix of files: some spanning several parts, some exactly one part, some tiny.
        final Map<String, byte[]> expected = new HashMap<>();
        final Set<String> multiPartFiles = new HashSet<>();
        try (Directory local = new NIOFSDirectory(createTempDir())) {
            for (int i = 0; i < 6; i++) {
                final String name = "_" + i + ".dat";
                final int payload;
                if (i < 3) {
                    payload = randomIntBetween((int) partSize + 1, (int) (partSize * 3));
                    multiPartFiles.add(name);
                } else if (i == 3) {
                    payload = (int) partSize - CodecUtil.footerLength(); // exactly one part including the footer
                } else {
                    payload = randomIntBetween(1, 4096);
                }
                final byte[] bytes = randomByteArrayOfLength(payload);
                try (IndexOutput out = local.createOutput(name, IOContext.DEFAULT)) {
                    out.writeBytes(bytes, bytes.length);
                    CodecUtil.writeFooter(out);
                }
                try (IndexInput in = local.openInput(name, IOContext.READONCE)) {
                    final byte[] onDisk = new byte[(int) in.length()];
                    in.readBytes(onDisk, 0, onDisk.length);
                    expected.put(name, onDisk);
                }
                remoteSource.copyFrom(local, name, name, IOContext.DEFAULT);
            }
        }
        for (String name : multiPartFiles) {
            assertThat(remoteSource.fileLength(name), greaterThan(partSize));
        }
        blobContainer.rangeReads.set(0);
        blobContainer.wholeReads.set(0);

        final ParallelDownloadPermits permits = recoverySettings.getRemoteStoreParallelDownloadPermits();
        try (Directory target = new NIOFSDirectory(createTempDir())) {
            final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            downloader.downloadAsync(new CancellableThreads(), remoteSource, target, expected.keySet(), future);
            future.actionGet();

            for (Map.Entry<String, byte[]> entry : expected.entrySet()) {
                try (IndexInput in = target.openInput(entry.getKey(), IOContext.READONCE)) {
                    final byte[] actual = new byte[(int) in.length()];
                    in.readBytes(actual, 0, actual.length);
                    assertArrayEquals("content of " + entry.getKey(), entry.getValue(), actual);
                    // The downloaded file must still carry a valid Lucene footer, i.e. checksum verification works.
                    in.seek(0);
                    CodecUtil.checksumEntireFile(in);
                }
            }
        }

        long expectedRangeReads = 0;
        for (String name : multiPartFiles) {
            final long length = expected.get(name).length;
            expectedRangeReads += (length + partSize - 1) / partSize;
        }
        assertEquals("one range request per part of every large file", expectedRangeReads, blobContainer.rangeReads.get());
        assertEquals("small files use the single-stream path", expected.size() - multiPartFiles.size(), blobContainer.wholeReads.get());
        assertEquals("all prefetch permits returned", maxParts, permits.getMaxPermits());
        assertBusy(() -> assertEquals(maxParts, availablePermits(permits)));
    }

    public void testMultiPartDownloadDisabledWithZeroBudget() throws Exception {
        final long partSize = ByteSizeUnit.MB.toBytes(1);
        final RecoverySettings recoverySettings = new RecoverySettings(
            Settings.builder()
                .put("indices.recovery.remote_store.parallel_download.part_size", new ByteSizeValue(partSize))
                .put("indices.recovery.remote_store.parallel_download.max_concurrent_parts", 0)
                .build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        final RemoteStoreFileDownloader downloader = new RemoteStoreFileDownloader(
            ShardId.fromString("[RemoteStoreFileDownloaderTests][0]"),
            threadPool,
            recoverySettings
        );
        final Path blobPath = createTempDir();
        final RangeCountingBlobContainer blobContainer = new RangeCountingBlobContainer(
            new FsBlobContainer(new FsBlobStore(8 * 1024, blobPath, false), BlobPath.cleanPath(), blobPath)
        );
        final RemoteSegmentStoreDirectory remoteSource = new RemoteSegmentStoreDirectory(
            new RemoteDirectory(blobContainer),
            mock(RemoteDirectory.class),
            mock(RemoteStoreLockManager.class),
            threadPool,
            ShardId.fromString("[RemoteStoreFileDownloaderTests][0]")
        );
        final String name = "_big.dat";
        final byte[] bytes = randomByteArrayOfLength((int) partSize * 2 + 17);
        try (Directory local = new NIOFSDirectory(createTempDir())) {
            try (IndexOutput out = local.createOutput(name, IOContext.DEFAULT)) {
                out.writeBytes(bytes, bytes.length);
                CodecUtil.writeFooter(out);
            }
            remoteSource.copyFrom(local, name, name, IOContext.DEFAULT);
        }
        blobContainer.rangeReads.set(0);
        blobContainer.wholeReads.set(0);
        try (Directory target = new NIOFSDirectory(createTempDir())) {
            final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            downloader.downloadAsync(new CancellableThreads(), remoteSource, target, Set.of(name), future);
            future.actionGet();
            assertEquals(remoteSource.fileLength(name), target.fileLength(name));
        }
        assertEquals(0, blobContainer.rangeReads.get());
        assertEquals(1, blobContainer.wholeReads.get());
    }

    private static int availablePermits(ParallelDownloadPermits permits) {
        return permits.availablePermits();
    }

    private static final class RangeCountingBlobContainer extends FilterBlobContainer {
        final AtomicInteger rangeReads = new AtomicInteger();
        final AtomicInteger wholeReads = new AtomicInteger();

        RangeCountingBlobContainer(BlobContainer delegate) {
            super(delegate);
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return child;
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            wholeReads.incrementAndGet();
            return super.readBlob(blobName);
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            rangeReads.incrementAndGet();
            return super.readBlob(blobName, position, length);
        }
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

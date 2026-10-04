/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.DirectoryFileTransferTracker;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteStoreFileDownloader;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;

/**
 * End-to-end checks that multi-part parallel downloads from a {@link RemoteSegmentStoreDirectory} still flow through
 * the regular {@code Directory#copyFrom} pipeline -- the property that lets the two mechanisms broken by the earlier
 * multi-stream download (OpenSearch PR #10519) keep working:
 * <ul>
 *   <li>recovery / replication statistics, produced by {@code ReplicationStatsDirectoryWrapper} and
 *       {@code Store.StoreDirectory}'s {@link DirectoryFileTransferTracker}, and</li>
 *   <li>Lucene checksum verification at write time via {@link Store#createVerifyingOutput}, with no re-read.</li>
 * </ul>
 * Each check has a negative control so a silently no-op'd wrapper cannot pass.
 */
public class RemoteStoreMultiPartDownloadPipelineTests extends OpenSearchTestCase {

    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT).build()
    );
    private static final ShardId SHARD_ID = ShardId.fromString("[index][0]");
    private static final long PART_SIZE = ByteSizeUnit.MB.toBytes(1);

    private ThreadPool threadPool;
    private RemoteStoreFileDownloader downloader;
    private Path blobPath;
    private RangeCountingBlobContainer blobContainer;
    private RemoteSegmentStoreDirectory remoteSource;
    /** local file name -> expected on-disk bytes (payload + Lucene footer) */
    private final Map<String, byte[]> expected = new HashMap<>();
    private String bigFile;
    private String smallFile;

    @Before
    public void setup() throws IOException {
        threadPool = new TestThreadPool(getTestName());
        final RecoverySettings recoverySettings = new RecoverySettings(
            Settings.builder()
                .put("indices.recovery.max_concurrent_remote_store_streams", 2)
                .put("indices.recovery.remote_store.parallel_download.part_size", new ByteSizeValue(PART_SIZE))
                .put("indices.recovery.remote_store.parallel_download.max_concurrent_parts", randomIntBetween(2, 6))
                .build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        downloader = new RemoteStoreFileDownloader(SHARD_ID, threadPool, recoverySettings);

        blobPath = createTempDir();
        blobContainer = new RangeCountingBlobContainer(
            new FsBlobContainer(new FsBlobStore(8 * 1024, blobPath, false), BlobPath.cleanPath(), blobPath)
        );
        remoteSource = new RemoteSegmentStoreDirectory(
            new RemoteDirectory(blobContainer),
            mock(RemoteDirectory.class),
            mock(RemoteStoreLockManager.class),
            threadPool,
            SHARD_ID
        );

        // One file spanning 3-4 parts (multi-part path) and one small file (single-stream path), both with real
        // Lucene footers so the remote metadata carries a genuine checksum.
        bigFile = "_big.cfs";
        smallFile = "_small.si";
        try (Directory local = new NIOFSDirectory(createTempDir())) {
            for (Map.Entry<String, Integer> e : Map.of(
                bigFile,
                randomIntBetween((int) PART_SIZE * 2 + 1, (int) PART_SIZE * 4 - 100),
                smallFile,
                randomIntBetween(1, 4096)
            ).entrySet()) {
                final byte[] payload = randomByteArrayOfLength(e.getValue());
                try (IndexOutput out = local.createOutput(e.getKey(), IOContext.DEFAULT)) {
                    out.writeBytes(payload, payload.length);
                    CodecUtil.writeFooter(out);
                }
                try (IndexInput in = local.openInput(e.getKey(), IOContext.READONCE)) {
                    final byte[] onDisk = new byte[(int) in.length()];
                    in.readBytes(onDisk, 0, onDisk.length);
                    expected.put(e.getKey(), onDisk);
                }
                remoteSource.copyFrom(local, e.getKey(), e.getKey(), IOContext.DEFAULT);
            }
        }
        blobContainer.reset();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
        assertTrue(threadPool.awaitTermination(5, TimeUnit.SECONDS));
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Recovery statistics
    // ---------------------------------------------------------------------------------------------------------------

    public void testReplicationStatsAreReportedForMultiPartDownloads() throws Exception {
        final Map<String, Long> progressBytes = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> progressCalls = new ConcurrentHashMap<>();
        try (Store store = new Store(SHARD_ID, INDEX_SETTINGS, new NIOFSDirectory(createTempDir()), new DummyShardLock(SHARD_ID))) {
            // Exactly the wrapper RemoteStoreReplicationSource#getSegmentFiles puts around the store directory.
            final Directory destination = new SegmentReplicationSource.ReplicationStatsDirectoryWrapper(
                store.directory(),
                (name, bytes) -> {
                    progressBytes.merge(name, bytes, Long::sum);
                    progressCalls.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
                }
            );

            final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            downloader.downloadAsync(new CancellableThreads(), remoteSource, destination, expected.keySet(), future);
            future.actionGet();

            // The big file really went through the multi-part path.
            assertThat(blobContainer.rangeReads.get(), greaterThan(1));
            assertEquals(1, blobContainer.wholeReads.get());

            // Per-file progress (the _cat/recovery "bytes recovered" feed) accounts for every byte of every file...
            for (Map.Entry<String, byte[]> e : expected.entrySet()) {
                assertEquals("progress bytes for " + e.getKey(), (long) e.getValue().length, (long) progressBytes.get(e.getKey()));
            }
            // ...and is reported incrementally while the large file streams, not once at the end.
            assertThat(progressCalls.get(bigFile).get(), greaterThan(1));

            // The store-level transfer tracker (behind the remote_store.download node stats) saw the same bytes.
            final long totalBytes = expected.values().stream().mapToLong(b -> b.length).sum();
            final DirectoryFileTransferTracker tracker = store.getDirectoryFileTransferTracker();
            assertEquals(totalBytes, tracker.getTransferredBytesStarted());
            assertEquals(totalBytes, tracker.getTransferredBytesSucceeded());
            assertEquals(0L, tracker.getTransferredBytesFailed());
            assertThat(tracker.getLastTransferTimestampMs(), greaterThan(0L));
        }
    }

    public void testReplicationStatsRecordFailedMultiPartDownload() throws Exception {
        // Fail the range read of one prefetched part of the big file so the multi-part download itself fails.
        blobContainer.failRangeReadsAtOrAbove(remoteSource.getExistingRemoteFilename(bigFile), PART_SIZE);
        try (Store store = new Store(SHARD_ID, INDEX_SETTINGS, new NIOFSDirectory(createTempDir()), new DummyShardLock(SHARD_ID))) {
            final Directory destination = new SegmentReplicationSource.ReplicationStatsDirectoryWrapper(store.directory(), (n, b) -> {});
            final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            downloader.downloadAsync(new CancellableThreads(), remoteSource, destination, Set.of(bigFile), future);
            final Exception e = expectThrows(Exception.class, future::actionGet);
            assertTrue("expected the injected failure in the cause chain, got " + e, hasMessageInChain(e, "injected"));

            final DirectoryFileTransferTracker tracker = store.getDirectoryFileTransferTracker();
            assertEquals(expected.get(bigFile).length, tracker.getTransferredBytesStarted());
            assertEquals(expected.get(bigFile).length, tracker.getTransferredBytesFailed());
            assertEquals(0L, tracker.getTransferredBytesSucceeded());
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Checksum verification
    // ---------------------------------------------------------------------------------------------------------------

    /**
     * Destination that verifies each file at write time the way peer recovery's {@code MultiFileWriter} does: the
     * bytes go through {@link Store#createVerifyingOutput} and {@link Store#verify} is called before close.
     */
    private Directory verifyingDestination(Store store) {
        return new FilterDirectory(store.directory()) {
            @Override
            public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
                final RemoteSegmentStoreDirectory.UploadedSegmentMetadata uploaded = remoteSource.getSegmentsUploadedToRemoteStore()
                    .get(src);
                final StoreFileMetadata metadata = new StoreFileMetadata(
                    dest,
                    uploaded.getLength(),
                    Store.digestToString(Long.parseLong(uploaded.getChecksum())),
                    Version.LATEST
                );
                try (IndexInput in = from.openInput(src, context); IndexOutput out = store.createVerifyingOutput(dest, metadata, context)) {
                    out.copyBytes(in, in.length());
                    Store.verify(out);
                }
            }
        };
    }

    public void testWriteTimeChecksumVerificationPassesWithoutReReadingTheFile() throws Exception {
        try (Store store = new Store(SHARD_ID, INDEX_SETTINGS, new NIOFSDirectory(createTempDir()), new DummyShardLock(SHARD_ID))) {
            final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            downloader.downloadAsync(new CancellableThreads(), remoteSource, verifyingDestination(store), expected.keySet(), future);
            future.actionGet();

            // Verified at write time: every byte of the big file was fetched exactly once, as parts, and never re-read.
            final long parts = (expected.get(bigFile).length + PART_SIZE - 1) / PART_SIZE;
            assertEquals(parts, blobContainer.rangeReads.get());
            assertEquals(1, blobContainer.wholeReads.get());
            assertEquals(expected.get(bigFile).length, blobContainer.rangeBytesRequested.get());

            for (Map.Entry<String, byte[]> e : expected.entrySet()) {
                try (IndexInput in = store.directory().openInput(e.getKey(), IOContext.READONCE)) {
                    final byte[] actual = new byte[(int) in.length()];
                    in.readBytes(actual, 0, actual.length);
                    assertArrayEquals(e.getValue(), actual);
                    in.seek(0);
                    CodecUtil.checksumEntireFile(in);
                }
            }
        }
    }

    public void testWriteTimeChecksumVerificationDetectsCorruptionInsideAPrefetchedPart() throws Exception {
        // Flip one byte in the middle of the SECOND part of the big blob (a prefetched, heap-buffered part), leaving
        // the footer intact -- a footer-only check would miss this; a checksum over the ordered stream must not.
        final Path blob = blobPath.resolve(remoteSource.getExistingRemoteFilename(bigFile));
        final long offset = PART_SIZE + randomLongBetween(0, PART_SIZE - 1);
        try (SeekableByteChannel ch = Files.newByteChannel(blob, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            final ByteBuffer one = ByteBuffer.allocate(1);
            ch.position(offset).read(one);
            one.flip();
            final byte flipped = (byte) (one.get() ^ 0xFF);
            ch.position(offset).write(ByteBuffer.wrap(new byte[] { flipped }));
        }

        try (Store store = new Store(SHARD_ID, INDEX_SETTINGS, new NIOFSDirectory(createTempDir()), new DummyShardLock(SHARD_ID))) {
            final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            downloader.downloadAsync(new CancellableThreads(), remoteSource, verifyingDestination(store), Set.of(bigFile), future);
            final Exception e = expectThrows(Exception.class, future::actionGet);
            assertNotNull("expected a CorruptIndexException from write-time verification, got " + e, unwrapCorruption(e));
            // The small, untouched file is unaffected.
            final PlainActionFuture<Void> ok = PlainActionFuture.newFuture();
            downloader.downloadAsync(new CancellableThreads(), remoteSource, verifyingDestination(store), Set.of(smallFile), ok);
            ok.actionGet();
        }
    }

    private static CorruptIndexException unwrapCorruption(Throwable t) {
        while (t != null) {
            if (t instanceof CorruptIndexException c) {
                return c;
            }
            t = t.getCause();
        }
        return null;
    }

    private static boolean hasMessageInChain(Throwable t, String needle) {
        while (t != null) {
            if (t.getMessage() != null && t.getMessage().contains(needle)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    // ---------------------------------------------------------------------------------------------------------------

    private static final class RangeCountingBlobContainer extends FilterBlobContainer {
        final AtomicInteger rangeReads = new AtomicInteger();
        final AtomicInteger wholeReads = new AtomicInteger();
        final AtomicLong rangeBytesRequested = new AtomicLong();
        private volatile String failBlob;
        private volatile long failAtOrAbove = Long.MAX_VALUE;

        RangeCountingBlobContainer(BlobContainer delegate) {
            super(delegate);
        }

        void reset() {
            rangeReads.set(0);
            wholeReads.set(0);
            rangeBytesRequested.set(0);
        }

        void failRangeReadsAtOrAbove(String blobName, long position) {
            this.failBlob = blobName;
            this.failAtOrAbove = position;
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
            if (blobName.equals(failBlob) && position >= failAtOrAbove) {
                throw new IOException("injected range read failure for " + blobName + " at " + position);
            }
            rangeReads.incrementAndGet();
            rangeBytesRequested.addAndGet(length);
            return super.readBlob(blobName, position, length);
        }
    }
}

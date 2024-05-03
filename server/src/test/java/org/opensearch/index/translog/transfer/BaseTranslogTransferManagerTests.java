/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.SetOnce;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.InputStreamWithMetadata;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.indices.DefaultRemoteStoreSettings;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.TRANSLOG;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;
import static org.opensearch.index.translog.transfer.TranslogTransferMetadata.METADATA_SEPARATOR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@LuceneTestCase.SuppressFileSystems("*")
public class BaseTranslogTransferManagerTests extends OpenSearchTestCase {

    private TransferService transferService;
    private ShardId shardId;
    private BlobPath remoteBaseTransferPath;
    private ThreadPool threadPool;
    private long primaryTerm;
    private long generation;
    private long minTranslogGeneration;
    private RemoteTranslogTransferTracker remoteTranslogTransferTracker;
    byte[] tlogBytes;
    byte[] ckpBytes;
    FileTransferTracker tracker;
    BaseTranslogTransferManager translogTransferManager;
    BaseTranslogTransferManager translogTransferManager2;
    long delayForBlobDownload;
    private final boolean shouldUploadTranslogCkpAsMetadata = false;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        primaryTerm = randomNonNegativeLong();
        generation = randomNonNegativeLong();
        shardId = mock(ShardId.class);
        when(shardId.getIndex()).thenReturn(new Index("index", "indexUUid"));
        minTranslogGeneration = randomLongBetween(0, generation);
        remoteBaseTransferPath = new BlobPath().add("base_path");
        transferService = mock(TransferService.class);
        threadPool = new TestThreadPool(getClass().getName());
        remoteTranslogTransferTracker = new RemoteTranslogTransferTracker(shardId, 20);
        tlogBytes = "Hello Translog".getBytes(StandardCharsets.UTF_8);
        ckpBytes = "Hello Checkpoint".getBytes(StandardCharsets.UTF_8);
        tracker = new FileTransferTracker(new ShardId("index", "indexUuid", 0), remoteTranslogTransferTracker, false);
        translogTransferManager = TranslogTransferManagerFactory.getTranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            shouldUploadTranslogCkpAsMetadata
        );

        translogTransferManager2 = TranslogTransferManagerFactory.getTranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            true
        );

        delayForBlobDownload = 1;
        when(transferService.downloadBlob(any(BlobPath.class), eq("translog-23.tlog"))).thenAnswer(invocation -> {
            Thread.sleep(delayForBlobDownload);
            return new ByteArrayInputStream(tlogBytes);
        });

        when(transferService.downloadBlob(any(BlobPath.class), eq("translog-23.ckp"))).thenAnswer(invocation -> {
            Thread.sleep(delayForBlobDownload);
            return new ByteArrayInputStream(ckpBytes);
        });
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    @SuppressWarnings("unchecked")
    public void testTransferSnapshot() throws Exception {
        AtomicInteger fileTransferSucceeded = new AtomicInteger();
        AtomicInteger fileTransferFailed = new AtomicInteger();
        AtomicInteger translogTransferSucceeded = new AtomicInteger();
        AtomicInteger translogTransferFailed = new AtomicInteger();

        doNothing().when(transferService)
            .uploadBlob(
                any(TransferFileSnapshot.class),
                eq(remoteBaseTransferPath.add(String.valueOf(primaryTerm))),
                any(WritePriority.class)
            );
        doAnswer(invocationOnMock -> {
            ActionListener<TransferFileSnapshot> listener = (ActionListener<TransferFileSnapshot>) invocationOnMock.getArguments()[2];
            Set<TransferFileSnapshot> transferFileSnapshots = (Set<TransferFileSnapshot>) invocationOnMock.getArguments()[0];
            transferFileSnapshots.forEach(listener::onResponse);
            return null;
        }).when(transferService).uploadBlobs(anySet(), anyMap(), any(ActionListener.class), any(WritePriority.class));

        FileTransferTracker fileTransferTracker = new FileTransferTracker(
            new ShardId("index", "indexUUid", 0),
            remoteTranslogTransferTracker,
            false
        ) {
            @Override
            public void onSuccess(TranslogCheckpointSnapshot fileSnapshot) {
                fileTransferSucceeded.incrementAndGet();
                super.onSuccess(fileSnapshot);
            }

            @Override
            public void onFailure(TranslogCheckpointSnapshot fileSnapshot, Exception e) {
                fileTransferFailed.incrementAndGet();
                super.onFailure(fileSnapshot, e);
            }

        };

        BaseTranslogTransferManager translogTransferManager = TranslogTransferManagerFactory.getTranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            fileTransferTracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            shouldUploadTranslogCkpAsMetadata
        );

        assertTrue(translogTransferManager.transferSnapshot(createTransferSnapshot(), new TranslogTransferListener() {
            @Override
            public void onUploadComplete(TransferSnapshot transferSnapshot) {
                translogTransferSucceeded.incrementAndGet();
            }

            @Override
            public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) {
                translogTransferFailed.incrementAndGet();
            }
        }));
        assertEquals(2, fileTransferSucceeded.get());
        assertEquals(0, fileTransferFailed.get());
        assertEquals(1, translogTransferSucceeded.get());
        assertEquals(0, translogTransferFailed.get());
        assertEquals(2, fileTransferTracker.allUploadedGeneration().size());
    }

    public void testTransferSnapshotOnUploadTimeout() throws Exception {
        doAnswer(invocationOnMock -> {
            Set<TransferFileSnapshot> transferFileSnapshots = invocationOnMock.getArgument(0);
            ActionListener<TransferFileSnapshot> listener = invocationOnMock.getArgument(2);
            Runnable runnable = () -> {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                for (TransferFileSnapshot transferFileSnapshot : transferFileSnapshots) {
                    listener.onResponse(transferFileSnapshot);
                }
            };
            Thread t = new Thread(runnable);
            t.start();
            return null;
        }).when(transferService).uploadBlobs(anySet(), anyMap(), any(ActionListener.class), any(WritePriority.class));
        FileTransferTracker fileTransferTracker = new FileTransferTracker(
            new ShardId("index", "indexUUid", 0),
            remoteTranslogTransferTracker,
            false
        );
        RemoteStoreSettings remoteStoreSettings = mock(RemoteStoreSettings.class);
        when(remoteStoreSettings.getClusterRemoteTranslogTransferTimeout()).thenReturn(new TimeValue(1));
        BaseTranslogTransferManager translogTransferManager = TranslogTransferManagerFactory.getTranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            fileTransferTracker,
            remoteTranslogTransferTracker,
            remoteStoreSettings,
            shouldUploadTranslogCkpAsMetadata
        );
        SetOnce<Exception> exception = new SetOnce<>();
        translogTransferManager.transferSnapshot(createTransferSnapshot(), new TranslogTransferListener() {
            @Override
            public void onUploadComplete(TransferSnapshot transferSnapshot) {}

            @Override
            public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) {
                exception.set(ex);
            }
        });
        assertNotNull(exception.get());
        assertTrue(exception.get() instanceof TranslogUploadFailedException);
        assertEquals("Timed out waiting for transfer of snapshot test-to-string to complete", exception.get().getMessage());
    }

    public void testTransferSnapshotOnThreadInterrupt() throws Exception {

        List<Thread> uploadThreadList = new ArrayList<>();
        doAnswer(invocationOnMock -> {
            SetOnce<Thread> uploadThread = new SetOnce<>();
            uploadThread.set(new Thread(() -> {
                ActionListener<TransferFileSnapshot> listener = invocationOnMock.getArgument(2);
                try {
                    Thread.sleep(31 * 1000);
                } catch (InterruptedException ignore) {
                    List<TransferFileSnapshot> list = new ArrayList<>(invocationOnMock.getArgument(0));
                    listener.onFailure(new FileTransferException(list.get(0), ignore));
                }
            }));
            uploadThreadList.add(uploadThread.get());
            uploadThread.get().start();
            return null;
        }).when(transferService).uploadBlobs(anySet(), anyMap(), any(ActionListener.class), any(WritePriority.class));
        FileTransferTracker fileTransferTracker = new FileTransferTracker(
            new ShardId("index", "indexUUid", 0),
            remoteTranslogTransferTracker,
            false
        );
        BaseTranslogTransferManager translogTransferManager = TranslogTransferManagerFactory.getTranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            fileTransferTracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            shouldUploadTranslogCkpAsMetadata
        );
        SetOnce<Exception> exception = new SetOnce<>();

        Thread thread = new Thread(() -> {
            try {
                translogTransferManager.transferSnapshot(createTransferSnapshot(), new TranslogTransferListener() {
                    @Override
                    public void onUploadComplete(TransferSnapshot transferSnapshot) {}

                    @Override
                    public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) {
                        exception.set(ex);
                    }
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        thread.start();

        Thread.sleep(1000);
        // Interrupt the thread
        thread.interrupt();
        assertBusy(() -> {
            assertNotNull(exception.get());
            assertTrue(exception.get() instanceof TranslogUploadFailedException);
            assertEquals("Failed to upload test-to-string", exception.get().getMessage());
        });
        uploadThreadList.forEach(Thread::interrupt);
    }

    private TransferSnapshot createTransferSnapshot() {
        return new TransferSnapshot() {
            @Override
            public TranslogTransferMetadata getTranslogTransferMetadata() {
                return new TranslogTransferMetadata(primaryTerm, generation, minTranslogGeneration, randomInt(5));
            }

            @Override
            public Set<TranslogCheckpointSnapshot> getTranslogCheckpointSnapshots() {
                try {
                    return Set.of(
                        new TranslogCheckpointSnapshot(
                            primaryTerm,
                            generation,
                            minTranslogGeneration,
                            createTempFile(Translog.TRANSLOG_FILE_PREFIX + generation, Translog.TRANSLOG_FILE_SUFFIX),
                            createTempFile(Translog.TRANSLOG_FILE_PREFIX + generation, Translog.CHECKPOINT_SUFFIX),
                            null,
                            null,
                            null,
                            generation
                        ),
                        new TranslogCheckpointSnapshot(
                            primaryTerm,
                            generation - 1,
                            minTranslogGeneration,
                            createTempFile(Translog.TRANSLOG_FILE_PREFIX + (generation - 1), Translog.TRANSLOG_FILE_SUFFIX),
                            createTempFile(Translog.TRANSLOG_FILE_PREFIX + (generation - 1), Translog.CHECKPOINT_SUFFIX),
                            null,
                            null,
                            null,
                            generation - 1
                        )
                    );
                } catch (IOException e) {
                    throw new AssertionError("Failed to create temp file", e);
                }
            }

            @Override
            public String toString() {
                return "test-to-string";
            }
        };
    }

    public void testReadMetadataNoFile() throws IOException {
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            List<BlobMetadata> bmList = new LinkedList<>();
            latchedActionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrder(any(BlobPath.class), eq(TranslogTransferMetadata.METADATA_PREFIX), anyInt(), any(ActionListener.class));

        assertNull(translogTransferManager.readMetadata());
        assertNoDownloadStats(false);
    }

    // This should happen most of the time -
    public void testReadMetadataFile() throws IOException {
        TranslogTransferMetadata metadata1 = new TranslogTransferMetadata(1, 1, 1, 2);
        String mdFilename1 = metadata1.getFileName();

        TranslogTransferMetadata metadata2 = new TranslogTransferMetadata(1, 0, 1, 2);
        String mdFilename2 = metadata2.getFileName();
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            List<BlobMetadata> bmList = new LinkedList<>();
            bmList.add(new PlainBlobMetadata(mdFilename1, 1));
            bmList.add(new PlainBlobMetadata(mdFilename2, 1));
            latchedActionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrder(any(BlobPath.class), eq(TranslogTransferMetadata.METADATA_PREFIX), anyInt(), any(ActionListener.class));

        TranslogTransferMetadata metadata = createTransferSnapshot().getTranslogTransferMetadata();
        long delayForMdDownload = 1;
        when(transferService.downloadBlob(any(BlobPath.class), eq(mdFilename1))).thenAnswer(invocation -> {
            Thread.sleep(delayForMdDownload);
            return new ByteArrayInputStream(translogTransferManager.getMetadataBytes(metadata));
        });

        assertEquals(metadata, translogTransferManager.readMetadata());

        assertEquals(translogTransferManager.getMetadataBytes(metadata).length, remoteTranslogTransferTracker.getDownloadBytesSucceeded());
        assertTrue(remoteTranslogTransferTracker.getTotalDownloadTimeInMillis() >= delayForMdDownload);
    }

    public void testReadMetadataReadException() throws IOException {
        TranslogTransferMetadata tm = new TranslogTransferMetadata(1, 1, 1, 2);
        String mdFilename = tm.getFileName();

        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            List<BlobMetadata> bmList = new LinkedList<>();
            bmList.add(new PlainBlobMetadata(mdFilename, 1));
            latchedActionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrder(any(BlobPath.class), eq(TranslogTransferMetadata.METADATA_PREFIX), anyInt(), any(ActionListener.class));

        when(transferService.downloadBlob(any(BlobPath.class), eq(mdFilename))).thenThrow(new IOException("Something went wrong"));

        assertThrows(IOException.class, translogTransferManager::readMetadata);
        assertNoDownloadStats(true);
    }

    public void testMetadataFileNameOrder() throws IOException {
        // asserting that new primary followed new generation are lexicographically smallest
        String mdFilenameGen1 = new TranslogTransferMetadata(1, 1, 1, 2).getFileName();
        String mdFilenameGen2 = new TranslogTransferMetadata(1, 2, 1, 2).getFileName();
        String mdFilenamePrimary2 = new TranslogTransferMetadata(2, 1, 1, 2).getFileName();
        List<String> metadataFiles = Arrays.asList(mdFilenameGen1, mdFilenameGen2, mdFilenamePrimary2);
        Collections.sort(metadataFiles);
        assertEquals(Arrays.asList(mdFilenamePrimary2, mdFilenameGen2, mdFilenameGen1), metadataFiles);
    }

    public void testReadMetadataListException() throws IOException {
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            latchedActionListener.onFailure(new IOException("Issue while listing"));
            return null;
        }).when(transferService)
            .listAllInSortedOrder(any(BlobPath.class), eq(TranslogTransferMetadata.METADATA_PREFIX), anyInt(), any(ActionListener.class));

        when(transferService.downloadBlob(any(BlobPath.class), any(String.class))).thenThrow(new IOException("Something went wrong"));

        assertThrows(IOException.class, translogTransferManager::readMetadata);
        assertNoDownloadStats(false);
    }

    public void testDownloadTranslog() throws IOException {
        Path location = createTempDir();
        mockResponseDownloadBlobWithMetadata_WithNULLMetadata();
        assertFalse(Files.exists(location.resolve("translog-23.tlog")));
        assertFalse(Files.exists(location.resolve("translog-23.ckp")));
        translogTransferManager.downloadTranslog("12", "23", location);
        assertTrue(Files.exists(location.resolve("translog-23.tlog")));
        assertTrue(Files.exists(location.resolve("translog-23.ckp")));
        assertTlogCkpDownloadStats();
    }

    public void mockResponseDownloadBlobWithMetadata_WithNULLMetadata() throws IOException {
        when(transferService.downloadBlobWithMetadata(any(BlobPath.class), eq("translog-23.tlog"))).thenAnswer(invocation -> {
            Thread.sleep(delayForBlobDownload);
            return new InputStreamWithMetadata(new ByteArrayInputStream(tlogBytes), null);
        });
    }

    public void testDownloadTranslog_When_CkpFileStoredAsMetadata() throws IOException {
        Path location = createTempDir();
        mockResponseDownloadBlobWithMetadata_WithCkpFileStoredAsMetadata();
        assertFalse(Files.exists(location.resolve("translog-23.tlog")));
        assertFalse(Files.exists(location.resolve("translog-23.ckp")));
        translogTransferManager.downloadTranslog("12", "23", location);
        assertTrue(Files.exists(location.resolve("translog-23.tlog")));
        assertTrue(Files.exists(location.resolve("translog-23.ckp")));
        assertTlogCkpDownloadStats_when_CkpFileStoredAsMetadata();
    }

    public void mockResponseDownloadBlobWithMetadata_WithCkpFileStoredAsMetadata() throws IOException {
        Map<String, String> metadata = TranslogCheckpointSnapshot.createMetadata(ckpBytes);
        when(transferService.downloadBlobWithMetadata(any(BlobPath.class), eq("translog-23.tlog"))).thenAnswer(invocation -> {
            Thread.sleep(delayForBlobDownload);
            return new InputStreamWithMetadata(new ByteArrayInputStream(tlogBytes), metadata);
        });
    }

    public void testDownloadTranslogAlreadyExists() throws IOException {
        Path location = createTempDir();
        Files.createFile(location.resolve("translog-23.tlog"));
        Files.createFile(location.resolve("translog-23.ckp"));
        mockResponseDownloadBlobWithMetadata_WithNULLMetadata();
        translogTransferManager.downloadTranslog("12", "23", location);
        verify(transferService).downloadBlobWithMetadata(any(BlobPath.class), eq("translog-23.tlog"));
        verify(transferService).downloadBlob(any(BlobPath.class), eq("translog-23.ckp"));
        assertTrue(Files.exists(location.resolve("translog-23.tlog")));
        assertTrue(Files.exists(location.resolve("translog-23.ckp")));
        assertTlogCkpDownloadStats();
    }

    public void testDownloadTranslogAlreadyExists_When_CkpFileStoredAsMetadata() throws IOException {
        Path location = createTempDir();
        Files.createFile(location.resolve("translog-23.tlog"));
        Files.createFile(location.resolve("translog-23.ckp"));
        mockResponseDownloadBlobWithMetadata_WithCkpFileStoredAsMetadata();
        translogTransferManager.downloadTranslog("12", "23", location);
        verify(transferService, times(1)).downloadBlobWithMetadata(any(BlobPath.class), eq("translog-23.tlog"));
        verify(transferService, times(0)).downloadBlob(any(BlobPath.class), eq("translog-23.ckp"));
        assertTrue(Files.exists(location.resolve("translog-23.tlog")));
        assertTrue(Files.exists(location.resolve("translog-23.ckp")));
        assertTlogCkpDownloadStats_when_CkpFileStoredAsMetadata();
    }

    public void testDownloadTranslogWithTrackerUpdated() throws IOException {
        Path location = createTempDir();
        String translogFile = "translog-23.tlog", checkpointFile = "translog-23.ckp";
        Files.createFile(location.resolve(translogFile));
        Files.createFile(location.resolve(checkpointFile));
        mockResponseDownloadBlobWithMetadata_WithNULLMetadata();
        translogTransferManager.downloadTranslog("12", "23", location);
        verify(transferService).downloadBlobWithMetadata(any(BlobPath.class), eq(translogFile));
        verify(transferService).downloadBlob(any(BlobPath.class), eq(checkpointFile));
        assertTrue(Files.exists(location.resolve(translogFile)));
        assertTrue(Files.exists(location.resolve(checkpointFile)));

        // Since the tracker already holds the files with success state, adding them with failed state would throw exception
        assertThrows(IllegalStateException.class, () -> tracker.add(translogFile, false));
        assertThrows(IllegalStateException.class, () -> tracker.add(checkpointFile, false));

        // Since the tracker already holds the files with success state, adding them with success state is allowed
        tracker.add(translogFile, true);
        tracker.add(checkpointFile, true);
        assertTlogCkpDownloadStats();
    }

    public void testDownloadTranslogWithTrackerUpdated_When_CkpFileStoredAsMetadata() throws IOException {
        Path location = createTempDir();
        String translogFile = "translog-23.tlog", checkpointFile = "translog-23.ckp";
        Files.createFile(location.resolve(translogFile));
        Files.createFile(location.resolve(checkpointFile));
        mockResponseDownloadBlobWithMetadata_WithCkpFileStoredAsMetadata();
        translogTransferManager.downloadTranslog("12", "23", location);

        verify(transferService, times(1)).downloadBlobWithMetadata(any(BlobPath.class), eq(translogFile));
        verify(transferService, times(0)).downloadBlob(any(BlobPath.class), eq(checkpointFile));
        assertTrue(Files.exists(location.resolve(translogFile)));
        assertTrue(Files.exists(location.resolve(checkpointFile)));

        // Since the tracker already holds the translog.tlog file, and generation with success state, adding them with failed state would
        // throw exception
        assertThrows(IllegalStateException.class, () -> tracker.add(translogFile, false));
        assertThrows(IllegalStateException.class, () -> tracker.addGeneration(23, false));

        // Since the tracker doesn't have translog.ckp file status updated. adding it Failed is allowed
        tracker.add(checkpointFile, false);

        // Since the tracker already holds the translog.tlog file, and generation with success state, adding them with success state is
        // allowed
        tracker.add(translogFile, true);
        tracker.addGeneration(23, true);
        assertTlogCkpDownloadStats_when_CkpFileStoredAsMetadata();
    }

    public void testDeleteTranslogSuccess() throws Exception {
        BlobStore blobStore = mock(BlobStore.class);
        BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);
        BlobStoreTransferService blobStoreTransferService = new BlobStoreTransferService(blobStore, threadPool);
        BaseTranslogTransferManager translogTransferManager = TranslogTransferManagerFactory.getTranslogTransferManager(
            shardId,
            blobStoreTransferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            shouldUploadTranslogCkpAsMetadata
        );
        String translogFile = "translog-19.tlog", checkpointFile = "translog-19.ckp";
        tracker.addGeneration(19, true);
        tracker.add(translogFile, true);
        tracker.add(checkpointFile, true);
        assertEquals(1, tracker.allUploadedGeneration().size());
        assertEquals(2, tracker.allUploaded().size());

        List<String> files = List.of(checkpointFile, translogFile);
        translogTransferManager.deleteGenerationAsync(primaryTerm, Set.of(19L), () -> {});
        assertBusy(() -> assertEquals(0, tracker.allUploadedGeneration().size()));
        assertBusy(() -> assertEquals(0, tracker.allUploaded().size()));
        verify(blobContainer).deleteBlobsIgnoringIfNotExists(eq(files));
    }

    public void testDeleteTranslogSuccess_when_ckpStoredAsMetadata() throws Exception {
        BlobStore blobStore = mock(BlobStore.class);
        BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);
        BlobStoreTransferService blobStoreTransferService = new BlobStoreTransferService(blobStore, threadPool);
        BaseTranslogTransferManager translogTransferManager = TranslogTransferManagerFactory.getTranslogTransferManager(
            shardId,
            blobStoreTransferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            shouldUploadTranslogCkpAsMetadata
        );
        String translogFile = "translog-19.tlog", checkpointFile = "translog-19.ckp";
        tracker.addGeneration(19, true);
        tracker.add(translogFile, true);
        // tracker.add(checkpointFile, true);
        assertEquals(1, tracker.allUploadedGeneration().size());
        assertEquals(1, tracker.allUploaded().size());

        List<String> files = List.of(checkpointFile, translogFile);
        List<String> verifyDeleteFilesList = List.of(translogFile);
        translogTransferManager.deleteGenerationAsync(primaryTerm, Set.of(19L), () -> {});
        assertBusy(() -> assertEquals(0, tracker.allUploadedGeneration().size()));
        assertBusy(() -> assertEquals(0, tracker.allUploaded().size()));
        // only translog.tlog file will be sent for delete.
        verify(blobContainer).deleteBlobsIgnoringIfNotExists(eq(verifyDeleteFilesList));
    }

    public void testDeleteStaleTranslogMetadata() {
        String tm1 = new TranslogTransferMetadata(1, 1, 1, 2).getFileName();
        String tm2 = new TranslogTransferMetadata(1, 2, 1, 2).getFileName();
        String tm3 = new TranslogTransferMetadata(2, 3, 1, 2).getFileName();
        doAnswer(invocation -> {
            ActionListener<List<BlobMetadata>> actionListener = invocation.getArgument(4);
            List<BlobMetadata> bmList = new LinkedList<>();
            bmList.add(new PlainBlobMetadata(tm1, 1));
            bmList.add(new PlainBlobMetadata(tm2, 1));
            bmList.add(new PlainBlobMetadata(tm3, 1));
            actionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrderAsync(
                eq(ThreadPool.Names.REMOTE_PURGE),
                any(BlobPath.class),
                eq(TranslogTransferMetadata.METADATA_PREFIX),
                anyInt(),
                any(ActionListener.class)
            );
        List<String> files = List.of(tm2, tm3);
        translogTransferManager.deleteStaleTranslogMetadataFilesAsync(() -> {
            verify(transferService).listAllInSortedOrderAsync(
                eq(ThreadPool.Names.REMOTE_PURGE),
                any(BlobPath.class),
                eq(TranslogTransferMetadata.METADATA_PREFIX),
                eq(Integer.MAX_VALUE),
                any()
            );
            verify(transferService).deleteBlobsAsync(
                eq(ThreadPool.Names.REMOTE_PURGE),
                any(BlobPath.class),
                eq(files),
                any(ActionListener.class)
            );
        });
    }

    public void testDeleteTranslogFailure() throws Exception {
        FileTransferTracker tracker = new FileTransferTracker(new ShardId("index", "indexUuid", 0), remoteTranslogTransferTracker, false);
        BlobStore blobStore = mock(BlobStore.class);
        BlobContainer blobContainer = mock(BlobContainer.class);
        doAnswer(invocation -> { throw new IOException("test exception"); }).when(blobStore).blobContainer(any(BlobPath.class));
        // when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);
        BlobStoreTransferService blobStoreTransferService = new BlobStoreTransferService(blobStore, threadPool);
        BaseTranslogTransferManager translogTransferManager = TranslogTransferManagerFactory.getTranslogTransferManager(
            shardId,
            blobStoreTransferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            shouldUploadTranslogCkpAsMetadata
        );
        String translogFile = "translog-19.tlog", checkpointFile = "translog-19.ckp";
        tracker.add(translogFile, true);
        tracker.add(checkpointFile, true);
        tracker.addGeneration(19, true);
        assertEquals(2, tracker.allUploaded().size());
        assertEquals(1, tracker.allUploadedGeneration().size());

        translogTransferManager.deleteGenerationAsync(primaryTerm, Set.of(19L), () -> {});
        assertEquals(1, tracker.allUploadedGeneration().size());
        assertEquals(2, tracker.allUploaded().size());
    }

    private void assertNoDownloadStats(boolean nonZeroUploadTime) {
        assertEquals(0, remoteTranslogTransferTracker.getDownloadBytesSucceeded());
        assertEquals(0, remoteTranslogTransferTracker.getTotalDownloadsSucceeded());
        assertEquals(0, remoteTranslogTransferTracker.getLastSuccessfulDownloadTimestamp());
        if (nonZeroUploadTime == false) {
            assertEquals(0, remoteTranslogTransferTracker.getTotalDownloadTimeInMillis());
        }
    }

    private void assertTlogCkpDownloadStats() {
        assertEquals(tlogBytes.length + ckpBytes.length, remoteTranslogTransferTracker.getDownloadBytesSucceeded());
        // Expect delay for both tlog and ckp file
        assertTrue(remoteTranslogTransferTracker.getTotalDownloadTimeInMillis() >= 2 * delayForBlobDownload);
    }

    private void assertTlogCkpDownloadStats_when_CkpFileStoredAsMetadata() {
        assertEquals(tlogBytes.length + ckpBytes.length, remoteTranslogTransferTracker.getDownloadBytesSucceeded());
        // Expect delay for both tlog and ckp file
        assertTrue(remoteTranslogTransferTracker.getTotalDownloadTimeInMillis() >= delayForBlobDownload);
    }

    public void testGetPrimaryTermAndGeneration() {
        String nodeId = UUID.randomUUID().toString();
        String tm = new TranslogTransferMetadata(1, 2, 1, 2, nodeId).getFileName();
        Tuple<Tuple<Long, Long>, String> actualOutput = TranslogTransferMetadata.getNodeIdByPrimaryTermAndGeneration(tm);
        assertEquals(1L, (long) (actualOutput.v1().v1()));
        assertEquals(2L, (long) (actualOutput.v1().v2()));
        assertEquals(String.valueOf(Objects.hash(nodeId)), actualOutput.v2());
    }

    public void testMetadataConflict() throws InterruptedException {
        TranslogTransferMetadata tm = new TranslogTransferMetadata(1, 1, 1, 2, "node--1");
        String mdFilename = tm.getFileName();
        long count = mdFilename.chars().filter(ch -> ch == METADATA_SEPARATOR.charAt(0)).count();
        // There should not be any `_` in mdFile name as it is used a separator .
        assertEquals(10, count);
        Thread.sleep(1);
        TranslogTransferMetadata tm2 = new TranslogTransferMetadata(1, 1, 1, 2, "node--2");
        String mdFilename2 = tm2.getFileName();

        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            List<BlobMetadata> bmList = new LinkedList<>();
            bmList.add(new PlainBlobMetadata(mdFilename, 1));
            bmList.add(new PlainBlobMetadata(mdFilename2, 1));
            latchedActionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrder(any(BlobPath.class), eq(TranslogTransferMetadata.METADATA_PREFIX), anyInt(), any(ActionListener.class));

        assertThrows(RuntimeException.class, translogTransferManager::readMetadata);
    }

    public void testTransferTranslogCheckpointSnapshotWithAllFilesUploaded() throws Exception {
        // Arrange
        Set<TranslogCheckpointSnapshot> toUpload = createTestTranslogCheckpointSnapshots();
        Map<Long, BlobPath> blobPathMap = new HashMap<>();
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failedCount = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(toUpload.size());

        doAnswer(invocationOnMock -> {
            Set<TransferFileSnapshot> transferFileSnapshots = invocationOnMock.getArgument(0);
            ActionListener<TransferFileSnapshot> listener = invocationOnMock.getArgument(2);
            for (TransferFileSnapshot fileSnapshot : transferFileSnapshots) {
                listener.onResponse(fileSnapshot);
                fileSnapshot.close();
            }
            return null;
        }).when(transferService).uploadBlobs(anySet(), anyMap(), any(ActionListener.class), any(WritePriority.class));

        LatchedActionListener<TranslogCheckpointSnapshot> listener = new LatchedActionListener<>(
            ActionListener.wrap(resp -> successCount.getAndIncrement(), ex -> failedCount.getAndIncrement()),
            latch
        );

        translogTransferManager.transferTranslogCheckpointSnapshot(toUpload, blobPathMap, listener, WritePriority.HIGH);
        assertEquals(successCount.get(), 2);
    }

    public void testTransferTranslogCheckpointSnapshotWithOneOfTheTwoFilesFailedForATranslogGeneration() throws Exception {
        // Arrange
        Set<TranslogCheckpointSnapshot> toUpload = new HashSet<>();
        Iterator<TranslogCheckpointSnapshot> itr = createTestTranslogCheckpointSnapshots().iterator();
        if (itr.hasNext()) {
            toUpload.add(itr.next());
        }
        Map<Long, BlobPath> blobPathMap = new HashMap<>();
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failedCount = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(toUpload.size());

        doAnswer(invocationOnMock -> {
            Set<TransferFileSnapshot> transferFileSnapshots = invocationOnMock.getArgument(0);
            ActionListener<TransferFileSnapshot> listener = invocationOnMock.getArgument(2);
            TransferFileSnapshot fileSnapshot1 = null;
            TransferFileSnapshot fileSnapshot2 = null;
            Iterator<TransferFileSnapshot> iterator = transferFileSnapshots.iterator();
            if (iterator.hasNext()) {
                fileSnapshot1 = iterator.next();
                listener.onFailure(new FileTransferException(fileSnapshot1, new Exception("test")));
                fileSnapshot1.close();
            }
            if (iterator.hasNext()) {
                fileSnapshot2 = iterator.next();
                listener.onResponse(fileSnapshot2);
                fileSnapshot2.close();
            }
            return null;
        }).when(transferService).uploadBlobs(anySet(), anyMap(), any(ActionListener.class), any(WritePriority.class));

        LatchedActionListener<TranslogCheckpointSnapshot> listener = new LatchedActionListener<>(
            ActionListener.wrap(resp -> successCount.getAndIncrement(), ex -> failedCount.getAndIncrement()),
            latch
        );

        translogTransferManager.transferTranslogCheckpointSnapshot(toUpload, blobPathMap, listener, WritePriority.HIGH);
        assertEquals(successCount.get(), 0);
        assertEquals(failedCount.get(), 1);
    }

    private Set<TranslogCheckpointSnapshot> createTestTranslogCheckpointSnapshots() {
        Set<TranslogCheckpointSnapshot> snapshots = new HashSet<>();
        try {
            Path translogFile = createTempFile(Translog.TRANSLOG_FILE_PREFIX + generation, Translog.TRANSLOG_FILE_SUFFIX);
            Path checkpointFile = createTempFile(Translog.TRANSLOG_FILE_PREFIX + generation, Translog.CHECKPOINT_SUFFIX);
            snapshots.add(
                new TranslogCheckpointSnapshot(
                    primaryTerm,
                    generation,
                    minTranslogGeneration,
                    translogFile,
                    checkpointFile,
                    null,
                    null,
                    null,
                    generation
                )
            );

            translogFile = createTempFile(Translog.TRANSLOG_FILE_PREFIX + (generation - 1), Translog.TRANSLOG_FILE_SUFFIX);
            checkpointFile = createTempFile(Translog.TRANSLOG_FILE_PREFIX + (generation - 1), Translog.CHECKPOINT_SUFFIX);
            snapshots.add(
                new TranslogCheckpointSnapshot(
                    primaryTerm,
                    generation - 1,
                    minTranslogGeneration,
                    translogFile,
                    checkpointFile,
                    null,
                    null,
                    null,
                    generation - 1
                )
            );
        } catch (IOException e) {
            throw new AssertionError("Failed to create temp file", e);
        }
        return snapshots;
    }
}

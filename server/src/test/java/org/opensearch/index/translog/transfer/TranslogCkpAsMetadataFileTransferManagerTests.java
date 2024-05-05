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
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.InputStreamWithMetadata;
import org.opensearch.common.blobstore.stream.write.WritePriority;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.TRANSLOG;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@LuceneTestCase.SuppressFileSystems("*")
public class TranslogCkpAsMetadataFileTransferManagerTests extends OpenSearchTestCase {

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
    TranslogTransferManager translogCkpAsMetadataTransferManager;
    long delayForBlobDownload;
    private final boolean ckpAsTranslogMetadata = true;
    private TranslogCheckpointSnapshot spyTranslogCheckpointSnapshot1;
    private TranslogCheckpointSnapshot spyTranslogCheckpointSnapshot2;

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
        tracker = new FileTransferTracker(new ShardId("index", "indexUuid", 0), remoteTranslogTransferTracker, ckpAsTranslogMetadata);
        translogCkpAsMetadataTransferManager = TranslogTransferManagerFactory.getTranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            ckpAsTranslogMetadata
        );

        Path translogFile1 = createTempFile(Translog.TRANSLOG_FILE_PREFIX + generation, Translog.TRANSLOG_FILE_SUFFIX);
        Path checkpointFile1 = createTempFile(Translog.TRANSLOG_FILE_PREFIX + generation, Translog.CHECKPOINT_SUFFIX);

        Path translogFile2 = createTempFile(Translog.TRANSLOG_FILE_PREFIX + (generation - 1), Translog.TRANSLOG_FILE_SUFFIX);
        Path checkpointFile2 = createTempFile(Translog.TRANSLOG_FILE_PREFIX + (generation - 1), Translog.CHECKPOINT_SUFFIX);

        TranslogCheckpointSnapshot translogCheckpointSnapshot1 = new TranslogCheckpointSnapshot(
            primaryTerm,
            generation,
            minTranslogGeneration,
            translogFile1,
            checkpointFile1,
            null,
            null,
            null,
            generation
        );

        TranslogCheckpointSnapshot translogCheckpointSnapshot2 = new TranslogCheckpointSnapshot(
            primaryTerm,
            generation - 1,
            minTranslogGeneration,
            translogFile2,
            checkpointFile2,
            null,
            null,
            null,
            generation - 1
        );

        spyTranslogCheckpointSnapshot1 = spy(translogCheckpointSnapshot1);
        spyTranslogCheckpointSnapshot2 = spy(translogCheckpointSnapshot2);

        Map<String, String> metadata = TranslogCheckpointSnapshot.createMetadata(ckpBytes);
        TransferFileSnapshot dummyTransferFileSnapshot1 = new TransferFileSnapshot(translogFile1, 1, null, metadata);
        TransferFileSnapshot dummyTransferFileSnapshot2 = new TransferFileSnapshot(translogFile2, 1, null, metadata);
        doReturn(dummyTransferFileSnapshot1).when(spyTranslogCheckpointSnapshot1).getTranslogFileSnapshotWithMetadata();
        doReturn(dummyTransferFileSnapshot2).when(spyTranslogCheckpointSnapshot2).getTranslogFileSnapshotWithMetadata();

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

        TranslogTransferManager translogTransferManager = TranslogTransferManagerFactory.getTranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            fileTransferTracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            ckpAsTranslogMetadata
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
        TranslogTransferManager translogTransferManager = TranslogTransferManagerFactory.getTranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            fileTransferTracker,
            remoteTranslogTransferTracker,
            remoteStoreSettings,
            ckpAsTranslogMetadata
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
        SetOnce<Thread> uploadThread = new SetOnce<>();
        doAnswer(invocationOnMock -> {
            uploadThread.set(new Thread(() -> {
                ActionListener<TransferFileSnapshot> listener = invocationOnMock.getArgument(2);
                try {
                    Thread.sleep(31 * 1000);
                } catch (InterruptedException ignore) {
                    List<TransferFileSnapshot> list = new ArrayList<>(invocationOnMock.getArgument(0));
                    listener.onFailure(new FileTransferException(list.get(0), ignore));
                }
            }));
            uploadThread.get().start();
            return null;
        }).when(transferService).uploadBlobs(anySet(), anyMap(), any(ActionListener.class), any(WritePriority.class));
        FileTransferTracker fileTransferTracker = new FileTransferTracker(
            new ShardId("index", "indexUUid", 0),
            remoteTranslogTransferTracker,
            false
        );
        TranslogTransferManager translogTransferManager = TranslogTransferManagerFactory.getTranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            fileTransferTracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            ckpAsTranslogMetadata
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
        uploadThread.get().interrupt();
    }

    private TransferSnapshot createTransferSnapshot() {
        return new TransferSnapshot() {
            @Override
            public TranslogTransferMetadata getTranslogTransferMetadata() {
                return new TranslogTransferMetadata(primaryTerm, generation, minTranslogGeneration, randomInt(5));
            }

            @Override
            public Set<TranslogCheckpointSnapshot> getTranslogCheckpointSnapshots() {
                return Set.of(spyTranslogCheckpointSnapshot1, spyTranslogCheckpointSnapshot2);
            }

            @Override
            public String toString() {
                return "test-to-string";
            }
        };
    }

    public void testDownloadTranslog_When_CkpFileStoredAsMetadata() throws IOException {
        Path location = createTempDir();
        mockDownloadBlobWithMetadataResponse_WithMetadataValueAsCkpData();
        assertFalse(Files.exists(location.resolve("translog-23.tlog")));
        assertFalse(Files.exists(location.resolve("translog-23.ckp")));
        translogCkpAsMetadataTransferManager.downloadTranslog("12", "23", location);
        assertTrue(Files.exists(location.resolve("translog-23.tlog")));
        assertTrue(Files.exists(location.resolve("translog-23.ckp")));
        assertTlogCkpDownloadStats_When_CkpFileStoredAsMetadata();
    }

    public void testDownloadTranslogAlreadyExists_When_CkpFileStoredAsMetadata() throws IOException {
        Path location = createTempDir();
        Files.createFile(location.resolve("translog-23.tlog"));
        Files.createFile(location.resolve("translog-23.ckp"));
        mockDownloadBlobWithMetadataResponse_WithMetadataValueAsCkpData();
        translogCkpAsMetadataTransferManager.downloadTranslog("12", "23", location);
        verify(transferService, times(1)).downloadBlobWithMetadata(any(BlobPath.class), eq("translog-23.tlog"));
        verify(transferService, times(0)).downloadBlob(any(BlobPath.class), eq("translog-23.ckp"));
        assertTrue(Files.exists(location.resolve("translog-23.tlog")));
        assertTrue(Files.exists(location.resolve("translog-23.ckp")));
        assertTlogCkpDownloadStats_When_CkpFileStoredAsMetadata();
    }

    public void testDownloadTranslogWithTrackerUpdated_When_CkpFileStoredAsMetadata() throws IOException {
        Path location = createTempDir();
        String translogFile = "translog-23.tlog", checkpointFile = "translog-23.ckp";
        Files.createFile(location.resolve(translogFile));
        Files.createFile(location.resolve(checkpointFile));
        mockDownloadBlobWithMetadataResponse_WithMetadataValueAsCkpData();
        translogCkpAsMetadataTransferManager.downloadTranslog("12", "23", location);

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
        assertTlogCkpDownloadStats_When_CkpFileStoredAsMetadata();
    }

    public void mockDownloadBlobWithMetadataResponse_WithMetadataValueAsCkpData() throws IOException {
        Map<String, String> metadata = TranslogCheckpointSnapshot.createMetadata(ckpBytes);
        when(transferService.downloadBlobWithMetadata(any(BlobPath.class), eq("translog-23.tlog"))).thenAnswer(invocation -> {
            Thread.sleep(delayForBlobDownload);
            return new InputStreamWithMetadata(new ByteArrayInputStream(tlogBytes), metadata);
        });
    }

    public void testDeleteTranslogSuccess_when_ckpStoredAsMetadata() throws Exception {
        BlobStore blobStore = mock(BlobStore.class);
        BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);
        BlobStoreTransferService blobStoreTransferService = new BlobStoreTransferService(blobStore, threadPool);
        TranslogTransferManager translogTransferManager = TranslogTransferManagerFactory.getTranslogTransferManager(
            shardId,
            blobStoreTransferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            ckpAsTranslogMetadata
        );
        String translogFile = "translog-19.tlog";
        tracker.addGeneration(19, true);
        tracker.add(translogFile, true);
        // tracker.add(checkpointFile, true);
        assertEquals(1, tracker.allUploadedGeneration().size());
        assertEquals(1, tracker.allUploaded().size());

        List<String> verifyDeleteFilesList = List.of(translogFile);
        translogTransferManager.deleteGenerationAsync(primaryTerm, Set.of(19L), () -> {});
        assertBusy(() -> assertEquals(0, tracker.allUploadedGeneration().size()));
        assertBusy(() -> assertEquals(0, tracker.allUploaded().size()));
        // only translog.tlog file will be sent for delete.
        verify(blobContainer).deleteBlobsIgnoringIfNotExists(eq(verifyDeleteFilesList));
    }

    private void assertTlogCkpDownloadStats_When_CkpFileStoredAsMetadata() {
        assertEquals(tlogBytes.length + ckpBytes.length, remoteTranslogTransferTracker.getDownloadBytesSucceeded());
        // Expect delay for both tlog and ckp file
        assertTrue(remoteTranslogTransferTracker.getTotalDownloadTimeInMillis() >= delayForBlobDownload);
    }

    public void testTransferTranslogCheckpointSnapshotWithAllFilesUploaded() throws Exception {
        // Arrange
        Set<TranslogCheckpointSnapshot> toUpload = createTestTranslogCheckpointSnapshots();
        Map<Long, BlobPath> blobPathMap = new HashMap<>();
        AtomicInteger successGenCount = new AtomicInteger();
        AtomicInteger failedGenCount = new AtomicInteger();
        AtomicInteger processedFilesCount = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(toUpload.size());

        doAnswer(invocationOnMock -> {
            Set<TransferFileSnapshot> transferFileSnapshots = invocationOnMock.getArgument(0);
            ActionListener<TransferFileSnapshot> listener = invocationOnMock.getArgument(2);
            for (TransferFileSnapshot fileSnapshot : transferFileSnapshots) {
                listener.onResponse(fileSnapshot);
                assertNotNull(fileSnapshot.getMetadata());
                processedFilesCount.getAndIncrement();
                fileSnapshot.close();
            }
            return null;
        }).when(transferService).uploadBlobs(anySet(), anyMap(), any(ActionListener.class), any(WritePriority.class));

        LatchedActionListener<TranslogCheckpointSnapshot> listener = new LatchedActionListener<>(
            ActionListener.wrap(resp -> successGenCount.getAndIncrement(), ex -> failedGenCount.getAndIncrement()),
            latch
        );

        translogCkpAsMetadataTransferManager.transferTranslogCheckpointSnapshot(toUpload, blobPathMap, listener);
        assertEquals(successGenCount.get(), 2);
        assertEquals(failedGenCount.get(), 0);
        assertEquals(processedFilesCount.get(), 2);
    }

    public void testTransferTranslogCheckpointSnapshotWithOneOfTheTwoFilesFailedForATranslogGeneration() throws Exception {
        // Arrange
        Set<TranslogCheckpointSnapshot> toUpload = createTestTranslogCheckpointSnapshots();
        Map<Long, BlobPath> blobPathMap = new HashMap<>();
        AtomicInteger successGenCount = new AtomicInteger();
        AtomicInteger failedGenCount = new AtomicInteger();
        AtomicInteger proccessedFilesCount = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(toUpload.size());

        doAnswer(invocationOnMock -> {
            Set<TransferFileSnapshot> transferFileSnapshots = invocationOnMock.getArgument(0);
            ActionListener<TransferFileSnapshot> listener = invocationOnMock.getArgument(2);
            TransferFileSnapshot fileSnapshot1 = null;
            TransferFileSnapshot fileSnapshot2 = null;
            Iterator<TransferFileSnapshot> iterator = transferFileSnapshots.iterator();
            if (iterator.hasNext()) {
                fileSnapshot1 = iterator.next();
                assertNotNull(fileSnapshot1.getMetadata());
                proccessedFilesCount.getAndIncrement();
                listener.onFailure(new FileTransferException(fileSnapshot1, new Exception("test")));
                fileSnapshot1.close();
            }
            if (iterator.hasNext()) {
                fileSnapshot2 = iterator.next();
                assertNotNull(fileSnapshot1.getMetadata());
                proccessedFilesCount.getAndIncrement();
                listener.onResponse(fileSnapshot2);
                fileSnapshot2.close();
            }
            return null;
        }).when(transferService).uploadBlobs(anySet(), anyMap(), any(ActionListener.class), any(WritePriority.class));

        LatchedActionListener<TranslogCheckpointSnapshot> listener = new LatchedActionListener<>(
            ActionListener.wrap(resp -> successGenCount.getAndIncrement(), ex -> failedGenCount.getAndIncrement()),
            latch
        );

        translogCkpAsMetadataTransferManager.transferTranslogCheckpointSnapshot(toUpload, blobPathMap, listener);
        assertEquals(successGenCount.get(), 1);
        assertEquals(failedGenCount.get(), 1);
        assertEquals(proccessedFilesCount.get(), 2);
    }

    private Set<TranslogCheckpointSnapshot> createTestTranslogCheckpointSnapshots() {
        return Set.of(spyTranslogCheckpointSnapshot1, spyTranslogCheckpointSnapshot2);
    }
}

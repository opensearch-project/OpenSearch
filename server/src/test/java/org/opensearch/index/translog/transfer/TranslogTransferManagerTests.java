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
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.TestTranslog;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogReader;
import org.opensearch.index.translog.transfer.FileSnapshot.CheckpointFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.indices.DefaultRemoteStoreSettings;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.TRANSLOG;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;
import static org.opensearch.index.translog.transfer.TranslogTransferManager.CHECKPOINT_FILE_DATA_KEY;
import static org.opensearch.index.translog.transfer.TranslogTransferMetadata.METADATA_SEPARATOR;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@LuceneTestCase.SuppressFileSystems("*")
public class TranslogTransferManagerTests extends OpenSearchTestCase {

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
    TranslogTransferManager translogTransferManager;
    long delayForBlobDownload;
    boolean isTranslogMetadataEnabled;

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
        tracker = new FileTransferTracker(new ShardId("index", "indexUuid", 0), remoteTranslogTransferTracker);
        isTranslogMetadataEnabled = false;
        translogTransferManager = new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            isTranslogMetadataEnabled
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
                Mockito.eq(remoteBaseTransferPath.add(String.valueOf(primaryTerm))),
                any(WritePriority.class),
                any()
            );
        doAnswer(invocationOnMock -> {
            ActionListener<TransferFileSnapshot> listener = (ActionListener<TransferFileSnapshot>) invocationOnMock.getArguments()[2];
            Set<TransferFileSnapshot> transferFileSnapshots = (Set<TransferFileSnapshot>) invocationOnMock.getArguments()[0];
            transferFileSnapshots.forEach(listener::onResponse);
            return null;
        }).when(transferService).uploadBlobs(anySet(), anyMap(), any(ActionListener.class), any(WritePriority.class), any());

        FileTransferTracker fileTransferTracker = new FileTransferTracker(
            new ShardId("index", "indexUUid", 0),
            remoteTranslogTransferTracker
        ) {
            @Override
            public void onSuccess(TransferFileSnapshot fileSnapshot) {
                fileTransferSucceeded.incrementAndGet();
                super.onSuccess(fileSnapshot);
            }

            @Override
            public void onFailure(TransferFileSnapshot fileSnapshot, Exception e) {
                fileTransferFailed.incrementAndGet();
                super.onFailure(fileSnapshot, e);
            }

        };

        TranslogTransferManager translogTransferManager = new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            fileTransferTracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            isTranslogMetadataEnabled
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
        }, null));
        assertEquals(4, fileTransferSucceeded.get());
        assertEquals(0, fileTransferFailed.get());
        assertEquals(1, translogTransferSucceeded.get());
        assertEquals(0, translogTransferFailed.get());
        assertEquals(4, fileTransferTracker.allUploaded().size());
    }

    public void testTransferSnapshotOnFileTransferUploadFail() throws Exception {
        AtomicInteger fileTransferSucceeded = new AtomicInteger();
        AtomicInteger fileTransferFailed = new AtomicInteger();
        AtomicInteger translogTransferSucceeded = new AtomicInteger();
        AtomicInteger translogTransferFailed = new AtomicInteger();

        doAnswer(invocationOnMock -> {
            ActionListener<TransferFileSnapshot> listener = (ActionListener<TransferFileSnapshot>) invocationOnMock.getArguments()[2];
            Set<TransferFileSnapshot> transferFileSnapshots = (Set<TransferFileSnapshot>) invocationOnMock.getArguments()[0];

            TransferFileSnapshot actualFileSnapshot = transferFileSnapshots.iterator().next();
            FileTransferException testException = new FileTransferException(
                actualFileSnapshot,
                new RuntimeException("FileTransferUploadNeedsToFail-Exception")
            );

            listener.onFailure(testException);
            transferFileSnapshots.stream().skip(1).forEach(listener::onResponse);
            return null;
        }).when(transferService).uploadBlobs(anySet(), anyMap(), any(ActionListener.class), any(WritePriority.class), any());

        FileTransferTracker fileTransferTracker = new FileTransferTracker(
            new ShardId("index", "indexUUid", 0),
            remoteTranslogTransferTracker
        ) {
            @Override
            public void onSuccess(TransferFileSnapshot fileSnapshot) {
                fileTransferSucceeded.incrementAndGet();
                super.onSuccess(fileSnapshot);
            }

            @Override
            public void onFailure(TransferFileSnapshot fileSnapshot, Exception e) {
                fileTransferFailed.incrementAndGet();
                super.onFailure(fileSnapshot, e);
            }
        };

        TranslogTransferManager translogTransferManager = new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            fileTransferTracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            isTranslogMetadataEnabled
        );

        SetOnce<Exception> exception = new SetOnce<>();
        assertFalse(translogTransferManager.transferSnapshot(createTransferSnapshot(), new TranslogTransferListener() {
            @Override
            public void onUploadComplete(TransferSnapshot transferSnapshot) {
                translogTransferSucceeded.incrementAndGet();
            }

            @Override
            public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) {
                translogTransferFailed.incrementAndGet();
                exception.set(ex);
            }
        }, null));

        assertNotNull(exception.get());
        assertTrue(exception.get() instanceof TranslogUploadFailedException);
        assertEquals("Failed to upload 1 files during transfer", exception.get().getMessage());
        assertEquals(0, exception.get().getSuppressed().length);
        assertEquals(3, fileTransferSucceeded.get());
        assertEquals(1, fileTransferFailed.get());
        assertEquals(0, translogTransferSucceeded.get());
        assertEquals(1, translogTransferFailed.get());
        assertEquals(3, fileTransferTracker.allUploaded().size());
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
        }).when(transferService).uploadBlobs(anySet(), anyMap(), any(ActionListener.class), any(WritePriority.class), any());
        FileTransferTracker fileTransferTracker = new FileTransferTracker(
            new ShardId("index", "indexUUid", 0),
            remoteTranslogTransferTracker
        );
        RemoteStoreSettings remoteStoreSettings = mock(RemoteStoreSettings.class);
        when(remoteStoreSettings.getClusterRemoteTranslogTransferTimeout()).thenReturn(new TimeValue(1));
        TranslogTransferManager translogTransferManager = new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            fileTransferTracker,
            remoteTranslogTransferTracker,
            remoteStoreSettings,
            isTranslogMetadataEnabled
        );
        SetOnce<Exception> exception = new SetOnce<>();
        translogTransferManager.transferSnapshot(createTransferSnapshot(), new TranslogTransferListener() {
            @Override
            public void onUploadComplete(TransferSnapshot transferSnapshot) {}

            @Override
            public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) {
                exception.set(ex);
            }
        }, null);
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
        }).when(transferService).uploadBlobs(anySet(), anyMap(), any(ActionListener.class), any(WritePriority.class), any());
        FileTransferTracker fileTransferTracker = new FileTransferTracker(
            new ShardId("index", "indexUUid", 0),
            remoteTranslogTransferTracker
        );
        TranslogTransferManager translogTransferManager = new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            fileTransferTracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            isTranslogMetadataEnabled
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
                }, null);
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

    @SuppressWarnings("unchecked")
    private void mockSuccessfulFileUploads() throws Exception {
        doAnswer(invocationOnMock -> {
            ActionListener<TransferFileSnapshot> listener = (ActionListener<TransferFileSnapshot>) invocationOnMock.getArguments()[2];
            Set<TransferFileSnapshot> transferFileSnapshots = (Set<TransferFileSnapshot>) invocationOnMock.getArguments()[0];
            transferFileSnapshots.forEach(listener::onResponse);
            return null;
        }).when(transferService).uploadBlobs(anySet(), anyMap(), any(ActionListener.class), any(WritePriority.class), any());
    }

    /**
     * The fence wrappers answer for both configurations. With no fence (fencing disabled) callers get the safe
     * defaults - never superseded, a handoff transfer is a no-op, an aborted handoff may resume - so gating code
     * never needs to know whether the feature is on. With a fence they delegate to it.
     */
    public void testFenceOwnershipWrappers() throws Exception {
        // Fencing disabled: no fence.
        TranslogTransferManager unfenced = fencedTransferManager(null, tracker);
        assertFalse(unfenced.isFenceSuperseded(primaryTerm));
        unfenced.transferFenceOwnership(primaryTerm, "target-alloc"); // no fence to hand over: a no-op
        assertTrue(unfenced.revertFenceOwnership(primaryTerm));

        // Fencing enabled: delegation to the fence.
        FsBlobContainer container = fenceContainer();
        RemoteStoreFence fence = new RemoteStoreFence(container, "node-1-alloc", "node-1", shardId);
        fence.validateAndAdvance(primaryTerm);
        TranslogTransferManager fenced = fencedTransferManager(fence, tracker);
        assertFalse(fenced.isFenceSuperseded(primaryTerm));
        fenced.transferFenceOwnership(primaryTerm, "target-alloc");
        assertTrue("the target never wrote, so the revert reclaims ownership", fenced.revertFenceOwnership(primaryTerm));
        // A strictly higher term supersedes this copy.
        new RemoteStoreFence(container, "node-2-alloc", "node-2", shardId).validateAndAdvance(primaryTerm + 1);
        assertTrue(fenced.isFenceSuperseded(primaryTerm));
    }

    private FsBlobContainer fenceContainer() throws IOException {
        Path repoPath = createTempDir();
        FsBlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, repoPath, false);
        return (FsBlobContainer) blobStore.blobContainer(BlobPath.cleanPath());
    }

    private TranslogTransferManager fencedTransferManager(RemoteStoreFence fence, FileTransferTracker fileTransferTracker) {
        return new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            fileTransferTracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            isTranslogMetadataEnabled,
            fence
        );
    }

    public void testTransferSnapshotAdvancesFence() throws Exception {
        mockSuccessfulFileUploads();
        RemoteStoreFence fence = new RemoteStoreFence(fenceContainer(), "node-1-alloc", "node-1", shardId);
        TranslogTransferManager manager = fencedTransferManager(fence, tracker);

        AtomicInteger uploadComplete = new AtomicInteger();
        AtomicReference<Exception> uploadFailure = new AtomicReference<>();
        TranslogTransferListener listener = new TranslogTransferListener() {
            @Override
            public void onUploadComplete(TransferSnapshot transferSnapshot) {
                uploadComplete.incrementAndGet();
            }

            @Override
            public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) {
                uploadFailure.set(ex);
            }
        };

        assertTrue(manager.transferSnapshot(createTransferSnapshot(), listener, null));
        assertEquals(primaryTerm, fence.getTerm());
        assertEquals(0, fence.getSeq());

        // A subsequent sync advances the same CAS chain
        assertTrue(manager.transferSnapshot(createTransferSnapshot(), listener, null));
        assertEquals(1, fence.getSeq());

        assertEquals(2, uploadComplete.get());
        assertNull(uploadFailure.get());
    }

    public void testTransferSnapshotFencedByNewOwnerAtSameTerm() throws Exception {
        mockSuccessfulFileUploads();
        FsBlobContainer container = fenceContainer();
        RemoteStoreFence fence = new RemoteStoreFence(container, "node-source-alloc", "node-source", shardId);
        TranslogTransferManager manager = fencedTransferManager(fence, tracker);

        AtomicInteger uploadComplete = new AtomicInteger();
        AtomicReference<Exception> uploadFailure = new AtomicReference<>();
        TranslogTransferListener listener = new TranslogTransferListener() {
            @Override
            public void onUploadComplete(TransferSnapshot transferSnapshot) {
                uploadComplete.incrementAndGet();
            }

            @Override
            public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) {
                uploadFailure.set(ex);
            }
        };

        assertTrue(manager.transferSnapshot(createTransferSnapshot(), listener, null));

        // Relocation target (or a new primary) takes over the fence out of band
        new RemoteStoreFence(container, "node-target-alloc", "node-target", shardId).validateAndAdvance(primaryTerm);

        assertFalse(manager.transferSnapshot(createTransferSnapshot(), listener, null));
        assertEquals(1, uploadComplete.get());
        assertNotNull(uploadFailure.get());
        assertTrue(uploadFailure.get().toString(), uploadFailure.get() instanceof TranslogFencedException);
        assertTrue(uploadFailure.get().getMessage(), uploadFailure.get().getMessage().contains("fenced"));
    }

    public void testTransferSnapshotFencedByHigherTermBeforeBootstrap() throws Exception {
        primaryTerm = randomLongBetween(1, 1000);
        mockSuccessfulFileUploads();
        FsBlobContainer container = fenceContainer();
        // A higher-term primary already owns the fence
        new RemoteStoreFence(container, "node-new-alloc", "node-new", shardId).validateAndAdvance(primaryTerm + 1);

        RemoteStoreFence stalePrimaryFence = new RemoteStoreFence(container, "node-old-alloc", "node-old", shardId);
        TranslogTransferManager manager = fencedTransferManager(stalePrimaryFence, tracker);

        AtomicReference<Exception> uploadFailure = new AtomicReference<>();
        assertFalse(manager.transferSnapshot(createTransferSnapshot(), new TranslogTransferListener() {
            @Override
            public void onUploadComplete(TransferSnapshot transferSnapshot) {
                throw new AssertionError("upload must not be acknowledged for a fenced primary");
            }

            @Override
            public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) {
                uploadFailure.set(ex);
            }
        }, null));

        assertNotNull(uploadFailure.get());
        assertTrue(uploadFailure.get().toString(), uploadFailure.get() instanceof TranslogFencedException);
    }

    /**
     * The fatal/retryable boundary: only a genuinely lost CAS may fail the shard. A transient repository error during
     * the fence CAS must surface as an ordinary retryable upload failure — never as {@link TranslogFencedException},
     * which callers treat as tragic — and the next sync must recover and claim the chain.
     */
    public void testTransientFenceErrorIsRetryableNotFatal() throws Exception {
        mockSuccessfulFileUploads();
        AtomicBoolean failNextCas = new AtomicBoolean(true);
        Path repoPath = createTempDir();
        FsBlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, repoPath, false);
        FsBlobContainer container = new FsBlobContainer(blobStore, BlobPath.cleanPath(), repoPath) {
            @Override
            public String writeBlobConditionally(String blobName, InputStream inputStream, long blobSize, String expectedVersionToken)
                throws IOException {
                if (failNextCas.getAndSet(false)) {
                    throw new IOException("simulated transient repository error");
                }
                return super.writeBlobConditionally(blobName, inputStream, blobSize, expectedVersionToken);
            }
        };
        RemoteStoreFence fence = new RemoteStoreFence(container, "node-1-alloc", "node-1", shardId);
        TranslogTransferManager manager = fencedTransferManager(fence, tracker);

        AtomicReference<Exception> uploadFailure = new AtomicReference<>();
        TranslogTransferListener listener = new TranslogTransferListener() {
            @Override
            public void onUploadComplete(TransferSnapshot transferSnapshot) {}

            @Override
            public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) {
                uploadFailure.set(ex);
            }
        };

        assertFalse(manager.transferSnapshot(createTransferSnapshot(), listener, null));
        assertNotNull(uploadFailure.get());
        assertFalse(
            "a transient fence error must not be classified as fenced: " + uploadFailure.get(),
            uploadFailure.get() instanceof TranslogFencedException
        );
        assertTrue(uploadFailure.get().toString(), uploadFailure.get() instanceof TranslogUploadFailedException);

        // The retry recovers: the fence bootstraps and the upload is acknowledged.
        uploadFailure.set(null);
        assertTrue(manager.transferSnapshot(createTransferSnapshot(), listener, null));
        assertNull(uploadFailure.get());
        assertEquals(primaryTerm, fence.getTerm());
        assertEquals(0, fence.getSeq());
    }

    public void testFenceValidationRunsAfterMetadataUpload() throws Exception {
        mockSuccessfulFileUploads();

        // Order witness: the CAS must be issued only after the metadata upload completed ("the chain gates the
        // ack, and the CAS follows the metadata"). A successful CAS then proves the metadata was already visible
        // when any later takeover reads its restore point. Issued concurrently instead, the CAS can win before a
        // takeover's sweep while the metadata PUT is still in flight; the takeover then reads a restore point
        // without this generation and the writer acknowledges an operation no recovery will ever resolve -
        // acked-write loss (RemoteStoreFence.tla in formal-models/ exhibits the trace with SEQUENCED = FALSE).
        AtomicBoolean metadataUploaded = new AtomicBoolean();
        AtomicBoolean casSawMetadataUploaded = new AtomicBoolean();

        Path repoPath = createTempDir();
        FsBlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, repoPath, false);
        FsBlobContainer container = new FsBlobContainer(blobStore, BlobPath.cleanPath(), repoPath) {
            @Override
            public String writeBlobConditionally(String blobName, InputStream inputStream, long blobSize, String expectedVersionToken)
                throws IOException {
                casSawMetadataUploaded.set(metadataUploaded.get());
                return super.writeBlobConditionally(blobName, inputStream, blobSize, expectedVersionToken);
            }
        };

        doAnswer(invocationOnMock -> {
            metadataUploaded.set(true);
            return null;
        }).when(transferService).uploadBlob(any(TransferFileSnapshot.class), any(BlobPath.class), any(WritePriority.class), any());

        RemoteStoreFence fence = new RemoteStoreFence(container, "node-1-alloc", "node-1", shardId);
        TranslogTransferManager manager = fencedTransferManager(fence, tracker);

        assertTrue(manager.transferSnapshot(createTransferSnapshot(), new TranslogTransferListener() {
            @Override
            public void onUploadComplete(TransferSnapshot transferSnapshot) {}

            @Override
            public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) {
                throw new AssertionError(ex);
            }
        }, null));

        assertTrue("fence CAS was issued before the metadata upload completed", casSawMetadataUploaded.get());
        assertEquals(0, fence.getSeq());
    }

    public void testTakeoverBetweenMetadataUploadAndCasIsNotAcknowledged() throws Exception {
        mockSuccessfulFileUploads();
        FsBlobContainer container = fenceContainer();
        RemoteStoreFence fence = new RemoteStoreFence(container, "node-old-alloc", "node-old", shardId);
        fence.validateAndAdvance(primaryTerm); // this copy owns the chain

        // The interleaving that lost acked writes under the concurrent design: a new primary claims the fence
        // while this upload's metadata PUT is in flight, then reads its restore point - which cannot include this
        // generation. Sequenced, the CAS comes after the metadata and finds the chain taken, so the operation is
        // refused rather than acknowledged: the metadata file is a harmless never-acknowledged orphan.
        doAnswer(invocationOnMock -> {
            new RemoteStoreFence(container, "node-new-alloc", "node-new", shardId).validateAndAdvance(primaryTerm + 1);
            return null;
        }).when(transferService).uploadBlob(any(TransferFileSnapshot.class), any(BlobPath.class), any(WritePriority.class), any());

        TranslogTransferManager manager = fencedTransferManager(fence, tracker);
        AtomicReference<Exception> uploadFailure = new AtomicReference<>();
        assertFalse(manager.transferSnapshot(createTransferSnapshot(), new TranslogTransferListener() {
            @Override
            public void onUploadComplete(TransferSnapshot transferSnapshot) {
                throw new AssertionError("an upload whose fence was taken over mid-flight must not be acknowledged");
            }

            @Override
            public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) {
                uploadFailure.set(ex);
            }
        }, null));
        assertNotNull(uploadFailure.get());
        assertTrue(uploadFailure.get().toString(), uploadFailure.get() instanceof TranslogFencedException);
    }

    public void testTransferSnapshotWithoutFenceDoesNotRequireConditionalWrites() throws Exception {
        mockSuccessfulFileUploads();
        // The default (unfenced) manager must behave exactly as before, i.e. no fence blob and no CAS requirement
        assertTrue(translogTransferManager.transferSnapshot(createTransferSnapshot(), new TranslogTransferListener() {
            @Override
            public void onUploadComplete(TransferSnapshot transferSnapshot) {}

            @Override
            public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) {
                throw new AssertionError(ex);
            }
        }, null));
    }

    private TransferSnapshot createTransferSnapshot() throws IOException {
        try {
            CheckpointFileSnapshot checkpointFileSnapshot1 = new CheckpointFileSnapshot(
                primaryTerm,
                generation,
                minTranslogGeneration,
                createTempFile(Translog.TRANSLOG_FILE_PREFIX + generation, Translog.CHECKPOINT_SUFFIX),
                null
            );
            CheckpointFileSnapshot checkpointFileSnapshot2 = new CheckpointFileSnapshot(
                primaryTerm,
                generation,
                minTranslogGeneration,
                createTempFile(Translog.TRANSLOG_FILE_PREFIX + (generation - 1), Translog.CHECKPOINT_SUFFIX),
                null
            );
            TranslogFileSnapshot translogFileSnapshot1 = new TranslogFileSnapshot(
                primaryTerm,
                generation,
                createTempFile(Translog.TRANSLOG_FILE_PREFIX + generation, Translog.TRANSLOG_FILE_SUFFIX),
                null
            );
            TranslogFileSnapshot translogFileSnapshot2 = new TranslogFileSnapshot(
                primaryTerm,
                generation - 1,
                createTempFile(Translog.TRANSLOG_FILE_PREFIX + (generation - 1), Translog.TRANSLOG_FILE_SUFFIX),
                null
            );

            return new TransferSnapshot() {
                @Override
                public Set<TransferFileSnapshot> getCheckpointFileSnapshots() {
                    return Set.of(checkpointFileSnapshot1, checkpointFileSnapshot2);
                }

                @Override
                public Set<TransferFileSnapshot> getTranslogFileSnapshots() {
                    return Set.of(translogFileSnapshot1, translogFileSnapshot2);
                }

                @Override
                public TranslogTransferMetadata getTranslogTransferMetadata() {
                    return new TranslogTransferMetadata(primaryTerm, generation, minTranslogGeneration, randomInt(5));
                }

                @Override
                public Set<TransferFileSnapshot> getTranslogFileSnapshotWithMetadata() throws IOException {
                    translogFileSnapshot1.setMetadataFileInputStream(checkpointFileSnapshot1.inputStream());
                    translogFileSnapshot2.setMetadataFileInputStream(checkpointFileSnapshot2.inputStream());
                    return Set.of(translogFileSnapshot1, translogFileSnapshot2);
                }

                @Override
                public String toString() {
                    return "test-to-string";
                }
            };
        } catch (Exception e) {
            throw new IOException("Failed to create transfer snapshot");
        }
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
        assertFalse(Files.exists(location.resolve("translog-23.tlog")));
        assertFalse(Files.exists(location.resolve("translog-23.ckp")));
        translogTransferManager.downloadTranslog("12", "23", location);
        assertTrue(Files.exists(location.resolve("translog-23.tlog")));
        assertTrue(Files.exists(location.resolve("translog-23.ckp")));
        assertTlogCkpDownloadStats();
    }

    public void testDownloadTranslogAlreadyExists() throws IOException {
        Path location = createTempDir();
        Files.createFile(location.resolve("translog-23.tlog"));
        Files.createFile(location.resolve("translog-23.ckp"));

        translogTransferManager.downloadTranslog("12", "23", location);

        verify(transferService).downloadBlob(any(BlobPath.class), eq("translog-23.tlog"));
        verify(transferService).downloadBlob(any(BlobPath.class), eq("translog-23.ckp"));
        assertTrue(Files.exists(location.resolve("translog-23.tlog")));
        assertTrue(Files.exists(location.resolve("translog-23.ckp")));
        assertTlogCkpDownloadStats();
    }

    public void testDownloadTranslogWithTrackerUpdated() throws IOException {
        Path location = createTempDir();
        String translogFile = "translog-23.tlog", checkpointFile = "translog-23.ckp";
        Files.createFile(location.resolve(translogFile));
        Files.createFile(location.resolve(checkpointFile));

        translogTransferManager.downloadTranslog("12", "23", location);

        verify(transferService).downloadBlob(any(BlobPath.class), eq(translogFile));
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

    /**
     * A local generation whose footer checksum equals the advertised one is reused: nothing is fetched from the
     * repository, and the tracker records both files exactly as a real download would.
     */
    public void testDownloadTranslogIfChangedReusesCurrentLocalGeneration() throws IOException {
        Path location = createTempDir();
        long checksum = createTranslogGeneration(location, 23, true);
        byte[] before = Files.readAllBytes(location.resolve("translog-23.tlog"));

        assertFalse(translogTransferManager.downloadTranslogIfChanged("12", "23", location, String.valueOf(checksum)));

        verify(transferService, times(0)).downloadBlob(any(BlobPath.class), any(String.class));
        verify(transferService, times(0)).downloadBlobWithMetadata(any(BlobPath.class), any(String.class));
        assertArrayEquals(before, Files.readAllBytes(location.resolve("translog-23.tlog")));
        assertTrue(tracker.uploaded("translog-23.tlog"));
        assertTrue(tracker.uploaded("translog-23.ckp"));
        assertNoDownloadStats(false);
    }

    /**
     * When the local copy cannot be proven current the generation is downloaded, and nothing is registered with the
     * tracker until that download has succeeded.
     */
    public void testDownloadTranslogIfChangedDownloadsWhenLocalGenerationIsNotCurrent() throws IOException {
        Path location = createTempDir();
        long checksum = createTranslogGeneration(location, 23, true);

        assertTrue(translogTransferManager.downloadTranslogIfChanged("12", "23", location, String.valueOf(checksum + 1)));

        verify(transferService).downloadBlob(any(BlobPath.class), eq("translog-23.tlog"));
        verify(transferService).downloadBlob(any(BlobPath.class), eq("translog-23.ckp"));
        assertArrayEquals(tlogBytes, Files.readAllBytes(location.resolve("translog-23.tlog")));
        assertTrue(tracker.uploaded("translog-23.tlog"));
        assertTrue(tracker.uploaded("translog-23.ckp"));
        assertTlogCkpDownloadStats();
    }

    /**
     * With checkpoint data carried as object metadata there is no remote {@code .ckp} object, so a downloaded
     * generation registers only the {@code .tlog} with the tracker. A reused generation must leave the tracker in
     * exactly the same state, so callers that iterate tracked files see no difference between the two paths.
     */
    public void testDownloadTranslogIfChangedInMetadataModeTracksSameFilesAsDownload() throws IOException {
        TranslogTransferManager metadataModeManager = new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            true
        );
        // Reference: what a real metadata-mode download registers.
        Path downloaded = createTempDir();
        mockDownloadBlobWithMetadataResponse();
        assertTrue(metadataModeManager.downloadTranslogIfChanged("12", "23", downloaded, null));
        assertTrue(Files.exists(downloaded.resolve("translog-23.ckp")));
        Set<String> afterDownload = tracker.allUploaded();
        assertEquals(Set.of("translog-23.tlog"), afterDownload);

        // A reused generation in a fresh tracker must register the very same set.
        FileTransferTracker reuseTracker = new FileTransferTracker(new ShardId("index", "indexUuid", 0), remoteTranslogTransferTracker);
        TranslogTransferManager reuseManager = new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            reuseTracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            true
        );
        Path reused = createTempDir();
        long checksum = createTranslogGeneration(reused, 23, true);
        assertFalse(reuseManager.downloadTranslogIfChanged("12", "23", reused, String.valueOf(checksum)));
        assertEquals(afterDownload, reuseTracker.allUploaded());
    }

    /**
     * A generation's two files are written one after the other, so a download that dies between them must not leave a
     * stale checkpoint beside a fresh translog: the next reconciliation reads the footer at the checkpoint's offset, and
     * a stale checkpoint with the same offset would locate the new footer and trust the pair. The download therefore
     * removes both local files before the remote is contacted, in either mode, so no partial outcome can be trusted.
     */
    public void testDownloadTranslogRemovesStaleLocalFilesBeforeFetching() throws IOException {
        for (boolean metadataMode : new boolean[] { false, true }) {
            TranslogTransferManager manager = new TranslogTransferManager(
                shardId,
                transferService,
                remoteBaseTransferPath.add(TRANSLOG.getName()),
                remoteBaseTransferPath.add(METADATA.getName()),
                tracker,
                remoteTranslogTransferTracker,
                DefaultRemoteStoreSettings.INSTANCE,
                metadataMode
            );
            Path location = createTempDir();
            long staleChecksum = createTranslogGeneration(location, 23, true);
            Path translogPath = location.resolve("translog-23.tlog");
            Path checkpointPath = location.resolve("translog-23.ckp");
            assertTrue(manager.isLocalGenerationCurrent(location, 23, String.valueOf(staleChecksum)));

            // The remote fails on the very first request; record what was still on disk at that moment.
            AtomicBoolean checkpointPresentAtFetch = new AtomicBoolean(true);
            AtomicBoolean translogPresentAtFetch = new AtomicBoolean(true);
            Answer<Object> failFirstFetch = invocation -> {
                checkpointPresentAtFetch.set(Files.exists(checkpointPath));
                translogPresentAtFetch.set(Files.exists(translogPath));
                throw new IOException("simulated failure before any byte was written");
            };
            when(transferService.downloadBlob(any(BlobPath.class), eq("translog-23.ckp"))).thenAnswer(failFirstFetch);
            when(transferService.downloadBlobWithMetadata(any(BlobPath.class), eq("translog-23.tlog"))).thenAnswer(failFirstFetch);

            expectThrows(IOException.class, () -> manager.downloadTranslog("12", "23", location));

            assertFalse("checkpoint must be gone before the remote is contacted", checkpointPresentAtFetch.get());
            assertFalse("translog must be gone before the remote is contacted", translogPresentAtFetch.get());
            assertFalse(Files.exists(checkpointPath));
            assertFalse(Files.exists(translogPath));
            // Whatever the next attempt finds, it cannot be trusted.
            assertFalse(manager.isLocalGenerationCurrent(location, 23, String.valueOf(staleChecksum)));
        }
    }

    /**
     * Every way in which the local copy can fail to prove it is identical to the remote one must fall back to a
     * download: checksum mismatch, footer-less file, missing checkpoint, checkpoint for a different generation,
     * truncation, or a remote that does not advertise a checksum for the generation at all.
     */
    public void testIsLocalGenerationCurrentFallsBackWhenLocalStateCannotBeTrusted() throws IOException {
        // Checksum mismatch (e.g. a stale generation left behind by an earlier incarnation of the shard).
        {
            Path location = createTempDir();
            long checksum = createTranslogGeneration(location, 1, true);
            assertFalse(translogTransferManager.isLocalGenerationCurrent(location, 1, String.valueOf(checksum + 1)));
            assertTrue(translogTransferManager.isLocalGenerationCurrent(location, 1, String.valueOf(checksum)));
        }
        // Local generation written before footers existed.
        {
            Path location = createTempDir();
            long checksum = createTranslogGeneration(location, 1, false);
            assertFalse(translogTransferManager.isLocalGenerationCurrent(location, 1, String.valueOf(checksum)));
        }
        // Remote does not know the checksum (metadata uploaded by an older node).
        {
            Path location = createTempDir();
            createTranslogGeneration(location, 1, true);
            assertFalse(translogTransferManager.isLocalGenerationCurrent(location, 1, null));
        }
        // Checkpoint file missing.
        {
            Path location = createTempDir();
            long checksum = createTranslogGeneration(location, 1, true);
            Files.delete(location.resolve(Translog.getCommitCheckpointFileName(1)));
            assertFalse(translogTransferManager.isLocalGenerationCurrent(location, 1, String.valueOf(checksum)));
        }
        // Checkpoint file belongs to another generation.
        {
            Path location = createTempDir();
            long checksum = createTranslogGeneration(location, 1, true);
            Files.delete(location.resolve(Translog.getCommitCheckpointFileName(1)));
            createTranslogGeneration(location, 2, true);
            Files.move(
                location.resolve(Translog.getCommitCheckpointFileName(2)),
                location.resolve(Translog.getCommitCheckpointFileName(1))
            );
            assertFalse(translogTransferManager.isLocalGenerationCurrent(location, 1, String.valueOf(checksum)));
        }
        // Translog file truncated after the checkpoint was written.
        {
            Path location = createTempDir();
            long checksum = createTranslogGeneration(location, 1, true);
            Path translogPath = location.resolve(Translog.getFilename(1));
            try (FileChannel channel = FileChannel.open(translogPath, StandardOpenOption.WRITE)) {
                channel.truncate(Files.size(translogPath) - 1);
            }
            assertFalse(translogTransferManager.isLocalGenerationCurrent(location, 1, String.valueOf(checksum)));
        }
        // Checkpoint file corrupt (CRC no longer matches). Checkpoint.read throws the unchecked
        // TranslogCorruptedException here, which must be treated as "cannot trust, download" rather than propagate
        // and fail the engine open - the directory is no longer wiped before reconciliation, so a stale local
        // checkpoint is a state the download now has to cope with.
        {
            Path location = createTempDir();
            long checksum = createTranslogGeneration(location, 23, true);
            Path checkpointPath = location.resolve(Translog.getCommitCheckpointFileName(23));
            byte[] bytes = Files.readAllBytes(checkpointPath);
            bytes[bytes.length / 2] ^= 0x1;
            Files.write(checkpointPath, bytes);
            assertFalse(translogTransferManager.isLocalGenerationCurrent(location, 23, String.valueOf(checksum)));
            // And the full path downloads it, replacing the corrupt local files.
            assertTrue(translogTransferManager.downloadTranslogIfChanged("12", "23", location, String.valueOf(checksum)));
            assertArrayEquals(ckpBytes, Files.readAllBytes(checkpointPath));
        }
    }

    private long createTranslogGeneration(Path location, long generation, boolean withFooter) throws IOException {
        return TestTranslog.createTranslogGeneration(random(), location, generation, withFooter);
    }

    public void testDeleteTranslogSuccess() throws Exception {
        BlobStore blobStore = mock(BlobStore.class);
        BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);
        BlobStoreTransferService blobStoreTransferService = new BlobStoreTransferService(blobStore, threadPool);
        TranslogTransferManager translogTransferManager = new TranslogTransferManager(
            shardId,
            blobStoreTransferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            isTranslogMetadataEnabled
        );
        String translogFile = "translog-19.tlog", checkpointFile = "translog-19.ckp";
        tracker.add(translogFile, true);
        tracker.add(checkpointFile, true);
        assertEquals(2, tracker.allUploaded().size());

        List<String> files = List.of(checkpointFile, translogFile);
        translogTransferManager.deleteGenerationAsync(primaryTerm, Set.of(19L), () -> {});
        assertBusy(() -> assertEquals(0, tracker.allUploaded().size()));
        verify(blobContainer).deleteBlobsIgnoringIfNotExists(eq(files));
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
        FileTransferTracker tracker = new FileTransferTracker(new ShardId("index", "indexUuid", 0), remoteTranslogTransferTracker);
        BlobStore blobStore = mock(BlobStore.class);
        BlobContainer blobContainer = mock(BlobContainer.class);
        doAnswer(invocation -> { throw new IOException("test exception"); }).when(blobStore).blobContainer(any(BlobPath.class));
        // when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);
        BlobStoreTransferService blobStoreTransferService = new BlobStoreTransferService(blobStore, threadPool);
        TranslogTransferManager translogTransferManager = new TranslogTransferManager(
            shardId,
            blobStoreTransferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            isTranslogMetadataEnabled
        );
        String translogFile = "translog-19.tlog", checkpointFile = "translog-19.ckp";
        tracker.add(translogFile, true);
        tracker.add(checkpointFile, true);
        assertEquals(2, tracker.allUploaded().size());

        translogTransferManager.deleteGenerationAsync(primaryTerm, Set.of(19L), () -> {});
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
        assertEquals(14, count);
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

    // tests for cases when ckp is stored as translog metadata.
    public void testTransferSnapshotWithTranslogMetadata() throws Exception {
        AtomicInteger fileTransferSucceeded = new AtomicInteger();
        AtomicInteger fileTransferFailed = new AtomicInteger();
        AtomicInteger translogTransferSucceeded = new AtomicInteger();
        AtomicInteger translogTransferFailed = new AtomicInteger();

        isTranslogMetadataEnabled = true;

        doNothing().when(transferService)
            .uploadBlob(
                any(TransferFileSnapshot.class),
                Mockito.eq(remoteBaseTransferPath.add(String.valueOf(primaryTerm))),
                any(WritePriority.class),
                any()
            );
        doAnswer(invocationOnMock -> {
            ActionListener<TransferFileSnapshot> listener = (ActionListener<TransferFileSnapshot>) invocationOnMock.getArguments()[2];
            Set<TransferFileSnapshot> transferFileSnapshots = (Set<TransferFileSnapshot>) invocationOnMock.getArguments()[0];
            transferFileSnapshots.forEach(transferFileSnapshot -> {
                assertNotNull(transferFileSnapshot.getMetadataFileInputStream());
                listener.onResponse(transferFileSnapshot);
            });
            return null;
        }).when(transferService).uploadBlobs(anySet(), anyMap(), any(ActionListener.class), any(WritePriority.class), any());

        FileTransferTracker fileTransferTracker = new FileTransferTracker(
            new ShardId("index", "indexUUid", 0),
            remoteTranslogTransferTracker
        ) {
            @Override
            public void onSuccess(TransferFileSnapshot fileSnapshot) {
                fileTransferSucceeded.incrementAndGet();
                super.onSuccess(fileSnapshot);
            }

            @Override
            public void onFailure(TransferFileSnapshot fileSnapshot, Exception e) {
                fileTransferFailed.incrementAndGet();
                super.onFailure(fileSnapshot, e);
            }

        };

        translogTransferManager = new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            fileTransferTracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            isTranslogMetadataEnabled
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
        }, null));
        assertEquals(2, fileTransferSucceeded.get());
        assertEquals(0, fileTransferFailed.get());
        assertEquals(1, translogTransferSucceeded.get());
        assertEquals(0, translogTransferFailed.get());
        assertEquals(2, fileTransferTracker.allUploaded().size());
    }

    public void testDownloadTranslogWithMetadata() throws IOException {
        isTranslogMetadataEnabled = true;
        translogTransferManager = new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            isTranslogMetadataEnabled
        );
        Path location = createTempDir();
        assertFalse(Files.exists(location.resolve("translog-23.tlog")));
        assertFalse(Files.exists(location.resolve("translog-23.ckp")));
        mockDownloadBlobWithMetadataResponse();
        translogTransferManager.downloadTranslog("12", "23", location);
        verify(transferService, times(0)).downloadBlob(any(BlobPath.class), eq("translog-23.tlog"));
        verify(transferService, times(0)).downloadBlob(any(BlobPath.class), eq("translog-23.ckp"));
        verify(transferService, times(1)).downloadBlobWithMetadata(any(BlobPath.class), eq("translog-23.tlog"));
        assertTrue(Files.exists(location.resolve("translog-23.tlog")));
        assertTrue(Files.exists(location.resolve("translog-23.ckp")));
        assertTlogCkpDownloadStatsWithMetadata();
    }

    public void testSynchronousUploadMetadataRecoversCheckpointWithoutCheckpointBlob() throws IOException {
        BlobContainer blobContainer = mock(BlobContainer.class);
        BlobStore blobStore = mock(BlobStore.class);
        when(blobStore.isBlobMetadataEnabled()).thenReturn(true);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);

        AtomicReference<byte[]> uploadedTranslog = new AtomicReference<>();
        AtomicReference<Map<String, String>> uploadedMetadata = new AtomicReference<>();
        doAnswer(invocation -> {
            uploadedTranslog.set(((InputStream) invocation.getArgument(1)).readAllBytes());
            uploadedMetadata.set(Map.copyOf(invocation.getArgument(4)));
            return null;
        }).when(blobContainer)
            .writeBlobWithMetadata(eq("translog-23.tlog"), any(InputStream.class), Mockito.anyLong(), eq(true), any(), Mockito.isNull());
        when(blobContainer.readBlobWithMetadata("translog-23.tlog")).thenAnswer(
            invocation -> new InputStreamWithMetadata(new ByteArrayInputStream(uploadedTranslog.get()), uploadedMetadata.get())
        );

        BlobStoreTransferService blobStoreTransferService = new BlobStoreTransferService(blobStore, threadPool);
        TransferFileSnapshot transferFileSnapshot = new TransferFileSnapshot("translog-23.tlog", tlogBytes, 12);
        transferFileSnapshot.setMetadataFileInputStream(new ByteArrayInputStream(ckpBytes));
        blobStoreTransferService.uploadBlob(
            transferFileSnapshot,
            remoteBaseTransferPath.add(TRANSLOG.getName()).add("12"),
            WritePriority.HIGH,
            null
        );

        TranslogTransferManager manager = new TranslogTransferManager(
            shardId,
            blobStoreTransferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            true
        );
        Path location = createTempDir();
        manager.downloadTranslog("12", "23", location);

        assertArrayEquals(tlogBytes, Files.readAllBytes(location.resolve("translog-23.tlog")));
        assertArrayEquals(ckpBytes, Files.readAllBytes(location.resolve("translog-23.ckp")));
        verify(blobContainer, times(1)).readBlobWithMetadata("translog-23.tlog");
        verify(blobContainer, times(0)).readBlob("translog-23.ckp");
    }

    private void mockDownloadBlobWithMetadataResponse() throws IOException {
        Map<String, String> metadata = new HashMap<>();
        String ckpDataString = Base64.getEncoder().encodeToString(ckpBytes);
        metadata.put(CHECKPOINT_FILE_DATA_KEY, ckpDataString);
        when(transferService.downloadBlobWithMetadata(any(BlobPath.class), eq("translog-23.tlog"))).thenAnswer(invocation -> {
            Thread.sleep(delayForBlobDownload);
            return new InputStreamWithMetadata(new ByteArrayInputStream(tlogBytes), metadata);
        });
    }

    private void assertTlogCkpDownloadStatsWithMetadata() {
        assertEquals(tlogBytes.length, remoteTranslogTransferTracker.getDownloadBytesSucceeded());
        // Expect delay for both tlog and ckp file
        assertTrue(remoteTranslogTransferTracker.getTotalDownloadTimeInMillis() >= delayForBlobDownload);
    }

    public void testlistTranslogMetadataFilesAsync() throws Exception {
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
        AtomicBoolean fetchCompleted = new AtomicBoolean(false);
        translogTransferManager.listTranslogMetadataFilesAsync(new ActionListener<>() {
            @Override
            public void onResponse(List<BlobMetadata> blobMetadata) {
                assertEquals(3, blobMetadata.size());
                assertEquals(blobMetadata.stream().map(BlobMetadata::name).collect(Collectors.toList()), List.of(tm1, tm2, tm3));
                fetchCompleted.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                fetchCompleted.set(true);
                throw new RuntimeException(e);
            }
        });
        assertBusy(() -> assertTrue(fetchCompleted.get()));
    }

    public void testReadMetadataForGivenTimestampNoFile() throws IOException {
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            List<BlobMetadata> bmList = new LinkedList<>();
            latchedActionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrder(any(BlobPath.class), eq(TranslogTransferMetadata.METADATA_PREFIX), anyInt(), any(ActionListener.class));

        assertNull(translogTransferManager.readMetadata(1234L));
        assertNoDownloadStats(false);
    }

    public void testReadMetadataForGivenTimestampNoMatchingFile() throws IOException {
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            String timestamp1 = RemoteStoreUtils.invertLong(2345L);
            BlobMetadata bm1 = new PlainBlobMetadata("metadata__1__12__" + timestamp1 + "__node1__1", 1);
            String timestamp2 = RemoteStoreUtils.invertLong(3456L);
            BlobMetadata bm2 = new PlainBlobMetadata("metadata__1__12__" + timestamp2 + "__node1__1", 1);
            List<BlobMetadata> bmList = List.of(bm1, bm2);
            latchedActionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrder(any(BlobPath.class), eq(TranslogTransferMetadata.METADATA_PREFIX), anyInt(), any(ActionListener.class));

        assertNull(translogTransferManager.readMetadata(1234L));
        assertNoDownloadStats(false);
    }

    public void testReadMetadataForGivenTimestampFile() throws IOException {
        AtomicReference<String> mdFilename1 = new AtomicReference<>();
        String timestamp1 = RemoteStoreUtils.invertLong(2345L);
        mdFilename1.set("metadata__1__12__" + timestamp1 + "__node1__1");
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            BlobMetadata bm1 = new PlainBlobMetadata(mdFilename1.get(), 1);
            String timestamp2 = RemoteStoreUtils.invertLong(3456L);
            BlobMetadata bm2 = new PlainBlobMetadata("metadata__1__12__" + timestamp2 + "__node1__1", 1);
            List<BlobMetadata> bmList = List.of(bm1, bm2);
            latchedActionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrder(any(BlobPath.class), eq(TranslogTransferMetadata.METADATA_PREFIX), anyInt(), any(ActionListener.class));

        TranslogTransferMetadata metadata = createTransferSnapshot().getTranslogTransferMetadata();
        long delayForMdDownload = 1;
        when(transferService.downloadBlob(any(BlobPath.class), eq(mdFilename1.get()))).thenAnswer(invocation -> {
            Thread.sleep(delayForMdDownload);
            return new ByteArrayInputStream(translogTransferManager.getMetadataBytes(metadata));
        });

        assertEquals(metadata, translogTransferManager.readMetadata(3000L));

        assertEquals(translogTransferManager.getMetadataBytes(metadata).length, remoteTranslogTransferTracker.getDownloadBytesSucceeded());
        assertTrue(remoteTranslogTransferTracker.getTotalDownloadTimeInMillis() >= delayForMdDownload);
    }

    public void testReadMetadataForGivenTimestampException() throws IOException {
        AtomicReference<String> mdFilename1 = new AtomicReference<>();
        String timestamp1 = RemoteStoreUtils.invertLong(2345L);
        mdFilename1.set("metadata__1__12__" + timestamp1 + "__node1__1");
        doAnswer(invocation -> {
            LatchedActionListener<List<BlobMetadata>> latchedActionListener = invocation.getArgument(3);
            BlobMetadata bm1 = new PlainBlobMetadata(mdFilename1.get(), 1);
            String timestamp2 = RemoteStoreUtils.invertLong(3456L);
            BlobMetadata bm2 = new PlainBlobMetadata("metadata__1__12__" + timestamp2 + "__node1__1", 1);
            List<BlobMetadata> bmList = List.of(bm1, bm2);
            latchedActionListener.onResponse(bmList);
            return null;
        }).when(transferService)
            .listAllInSortedOrder(any(BlobPath.class), eq(TranslogTransferMetadata.METADATA_PREFIX), anyInt(), any(ActionListener.class));

        when(transferService.downloadBlob(any(BlobPath.class), eq(mdFilename1.get()))).thenThrow(new IOException("Something went wrong"));

        assertThrows(IOException.class, () -> translogTransferManager.readMetadata(3000L));
        assertNoDownloadStats(true);
    }

    public void testPopulateFileTrackerWithLocalStateNoReaders() {
        translogTransferManager.populateFileTrackerWithLocalState(null);
        assertTrue(translogTransferManager.getFileTransferTracker().allUploaded().isEmpty());

        translogTransferManager.populateFileTrackerWithLocalState(List.of());
        assertTrue(translogTransferManager.getFileTransferTracker().allUploaded().isEmpty());
    }

    public void testPopulateFileTrackerWithLocalState() {
        TranslogReader reader1 = mock(TranslogReader.class);
        when(reader1.getGeneration()).thenReturn(12L);
        TranslogReader reader2 = mock(TranslogReader.class);
        when(reader2.getGeneration()).thenReturn(23L);
        TranslogReader reader3 = mock(TranslogReader.class);
        when(reader3.getGeneration()).thenReturn(34L);
        TranslogReader reader4 = mock(TranslogReader.class);
        when(reader4.getGeneration()).thenReturn(45L);

        translogTransferManager.populateFileTrackerWithLocalState(List.of(reader1, reader2, reader3, reader4));
        assertEquals(
            Set.of(
                "translog-12.tlog",
                "translog-12.ckp",
                "translog-23.tlog",
                "translog-23.ckp",
                "translog-34.tlog",
                "translog-34.ckp",
                "translog-45.tlog",
                "translog-45.ckp"
            ),
            translogTransferManager.getFileTransferTracker().allUploaded()
        );
    }

    public void testPopulateFileTrackerWithLocalStateUsingTranslogMetadata() {
        TranslogTransferManager translogTransferManager = new TranslogTransferManager(
            shardId,
            transferService,
            remoteBaseTransferPath.add(TRANSLOG.getName()),
            remoteBaseTransferPath.add(METADATA.getName()),
            tracker,
            remoteTranslogTransferTracker,
            DefaultRemoteStoreSettings.INSTANCE,
            true
        );

        TranslogReader reader1 = mock(TranslogReader.class);
        when(reader1.getGeneration()).thenReturn(12L);
        TranslogReader reader2 = mock(TranslogReader.class);
        when(reader2.getGeneration()).thenReturn(23L);

        translogTransferManager.populateFileTrackerWithLocalState(List.of(reader1, reader2));
        assertEquals(Set.of("translog-12.tlog", "translog-23.tlog"), translogTransferManager.getFileTransferTracker().allUploaded());
    }
}

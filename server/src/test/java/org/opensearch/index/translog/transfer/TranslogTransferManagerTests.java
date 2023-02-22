/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Constants;
import org.mockito.Mockito;
import org.opensearch.action.ActionListener;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.util.set.Sets;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.transfer.FileSnapshot.CheckpointFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@LuceneTestCase.SuppressFileSystems("*")
public class TranslogTransferManagerTests extends OpenSearchTestCase {

    private TransferService transferService;
    private BlobPath remoteBaseTransferPath;
    private ThreadPool threadPool;
    private long primaryTerm;
    private long generation;
    private long minTranslogGeneration;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Muting this test on windows until https://github.com/opensearch-project/OpenSearch/issues/5923
        assumeFalse("Test does not run on Windows", Constants.WINDOWS);
        primaryTerm = randomNonNegativeLong();
        generation = randomNonNegativeLong();
        minTranslogGeneration = randomLongBetween(0, generation);
        remoteBaseTransferPath = new BlobPath().add("base_path");
        transferService = mock(TransferService.class);
        threadPool = new TestThreadPool(getClass().getName());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    @SuppressWarnings("unchecked")
    public void testTransferSnapshot() throws IOException {
        AtomicInteger fileTransferSucceeded = new AtomicInteger();
        AtomicInteger fileTransferFailed = new AtomicInteger();
        AtomicInteger translogTransferSucceeded = new AtomicInteger();
        AtomicInteger translogTransferFailed = new AtomicInteger();

        doNothing().when(transferService)
            .uploadBlob(any(TransferFileSnapshot.class), Mockito.eq(remoteBaseTransferPath.add(String.valueOf(primaryTerm))));
        doAnswer(invocationOnMock -> {
            ActionListener<TransferFileSnapshot> listener = (ActionListener<TransferFileSnapshot>) invocationOnMock.getArguments()[3];
            listener.onResponse((TransferFileSnapshot) invocationOnMock.getArguments()[1]);
            return null;
        }).when(transferService)
            .uploadBlobAsync(any(String.class), any(TransferFileSnapshot.class), any(BlobPath.class), any(ActionListener.class));

        FileTransferTracker fileTransferTracker = new FileTransferTracker(new ShardId("index", "indexUUid", 0)) {
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
            transferService,
            remoteBaseTransferPath,
            fileTransferTracker
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
        assertEquals(4, fileTransferSucceeded.get());
        assertEquals(0, fileTransferFailed.get());
        assertEquals(1, translogTransferSucceeded.get());
        assertEquals(0, translogTransferFailed.get());
        assertEquals(4, fileTransferTracker.allUploaded().size());
    }

    private TransferSnapshot createTransferSnapshot() {
        return new TransferSnapshot() {
            @Override
            public Set<TransferFileSnapshot> getCheckpointFileSnapshots() {
                try {
                    return Set.of(
                        new CheckpointFileSnapshot(
                            primaryTerm,
                            generation,
                            minTranslogGeneration,
                            createTempFile(Translog.TRANSLOG_FILE_PREFIX + generation, Translog.CHECKPOINT_SUFFIX)
                        ),
                        new CheckpointFileSnapshot(
                            primaryTerm,
                            generation,
                            minTranslogGeneration,
                            createTempFile(Translog.TRANSLOG_FILE_PREFIX + (generation - 1), Translog.CHECKPOINT_SUFFIX)
                        )
                    );
                } catch (IOException e) {
                    throw new AssertionError("Failed to create temp file", e);
                }
            }

            @Override
            public Set<TransferFileSnapshot> getTranslogFileSnapshots() {
                try {
                    return Set.of(
                        new TranslogFileSnapshot(
                            primaryTerm,
                            generation,
                            createTempFile(Translog.TRANSLOG_FILE_PREFIX + generation, Translog.TRANSLOG_FILE_SUFFIX)
                        ),
                        new TranslogFileSnapshot(
                            primaryTerm,
                            generation - 1,
                            createTempFile(Translog.TRANSLOG_FILE_PREFIX + (generation - 1), Translog.TRANSLOG_FILE_SUFFIX)
                        )
                    );
                } catch (IOException e) {
                    throw new AssertionError("Failed to create temp file", e);
                }
            }

            @Override
            public TranslogTransferMetadata getTranslogTransferMetadata() {
                return new TranslogTransferMetadata(primaryTerm, generation, minTranslogGeneration, randomInt(5));
            }
        };
    }

    public void testReadMetadataNoFile() throws IOException {
        TranslogTransferManager translogTransferManager = new TranslogTransferManager(transferService, remoteBaseTransferPath, null);

        when(transferService.listAll(remoteBaseTransferPath)).thenReturn(Sets.newHashSet());
        assertNull(translogTransferManager.readMetadata());
    }

    public void testReadMetadataSingleFile() throws IOException {
        TranslogTransferManager translogTransferManager = new TranslogTransferManager(transferService, remoteBaseTransferPath, null);

        // BlobPath does not have equals method, so we can't use the instance directly in when
        when(transferService.listAll(any(BlobPath.class))).thenReturn(Sets.newHashSet("12__234"));

        TranslogTransferMetadata metadata = createTransferSnapshot().getTranslogTransferMetadata();
        when(transferService.downloadBlob(any(BlobPath.class), eq("12__234"))).thenReturn(
            new ByteArrayInputStream(metadata.createMetadataBytes())
        );

        assertEquals(metadata, translogTransferManager.readMetadata());
    }

    public void testReadMetadataMultipleFiles() throws IOException {
        TranslogTransferManager translogTransferManager = new TranslogTransferManager(transferService, remoteBaseTransferPath, null);

        when(transferService.listAll(any(BlobPath.class))).thenReturn(Sets.newHashSet("12__234", "12__235", "12__233"));

        TranslogTransferMetadata metadata = createTransferSnapshot().getTranslogTransferMetadata();
        when(transferService.downloadBlob(any(BlobPath.class), eq("12__235"))).thenReturn(
            new ByteArrayInputStream(metadata.createMetadataBytes())
        );

        assertEquals(metadata, translogTransferManager.readMetadata());
    }

    public void testReadMetadataException() throws IOException {
        TranslogTransferManager translogTransferManager = new TranslogTransferManager(transferService, remoteBaseTransferPath, null);

        when(transferService.listAll(any(BlobPath.class))).thenReturn(Sets.newHashSet("12__234", "12__235", "12__233"));

        when(transferService.downloadBlob(any(BlobPath.class), eq("12__235"))).thenThrow(new IOException("Something went wrong"));

        assertNull(translogTransferManager.readMetadata());
    }

    public void testReadMetadataSamePrimaryTermGeneration() throws IOException {
        List<String> metadataFiles = Arrays.asList("12__234", "12__235", "12__234");
        assertThrows(IllegalArgumentException.class, () -> metadataFiles.sort(TranslogTransferMetadata.METADATA_FILENAME_COMPARATOR));
    }

    public void testDownloadTranslog() throws IOException {
        Path location = createTempDir();
        TranslogTransferManager translogTransferManager = new TranslogTransferManager(
            transferService,
            remoteBaseTransferPath,
            new FileTransferTracker(new ShardId("index", "indexUuid", 0))
        );

        when(transferService.downloadBlob(any(BlobPath.class), eq("translog-23.tlog"))).thenReturn(
            new ByteArrayInputStream("Hello Translog".getBytes(StandardCharsets.UTF_8))
        );

        when(transferService.downloadBlob(any(BlobPath.class), eq("translog-23.ckp"))).thenReturn(
            new ByteArrayInputStream("Hello Checkpoint".getBytes(StandardCharsets.UTF_8))
        );

        assertFalse(Files.exists(location.resolve("translog-23.tlog")));
        assertFalse(Files.exists(location.resolve("translog-23.ckp")));
        translogTransferManager.downloadTranslog("12", "23", location);
        assertTrue(Files.exists(location.resolve("translog-23.tlog")));
        assertTrue(Files.exists(location.resolve("translog-23.ckp")));
    }

    public void testDownloadTranslogAlreadyExists() throws IOException {
        FileTransferTracker tracker = new FileTransferTracker(new ShardId("index", "indexUuid", 0));
        Path location = createTempDir();
        Files.createFile(location.resolve("translog-23.tlog"));
        Files.createFile(location.resolve("translog-23.ckp"));

        TranslogTransferManager translogTransferManager = new TranslogTransferManager(transferService, remoteBaseTransferPath, tracker);

        when(transferService.downloadBlob(any(BlobPath.class), eq("translog-23.tlog"))).thenReturn(
            new ByteArrayInputStream("Hello Translog".getBytes(StandardCharsets.UTF_8))
        );
        when(transferService.downloadBlob(any(BlobPath.class), eq("translog-23.ckp"))).thenReturn(
            new ByteArrayInputStream("Hello Checkpoint".getBytes(StandardCharsets.UTF_8))
        );

        translogTransferManager.downloadTranslog("12", "23", location);

        verify(transferService).downloadBlob(any(BlobPath.class), eq("translog-23.tlog"));
        verify(transferService).downloadBlob(any(BlobPath.class), eq("translog-23.ckp"));
        assertTrue(Files.exists(location.resolve("translog-23.tlog")));
        assertTrue(Files.exists(location.resolve("translog-23.ckp")));
    }

    public void testDownloadTranslogWithTrackerUpdated() throws IOException {
        FileTransferTracker tracker = new FileTransferTracker(new ShardId("index", "indexUuid", 0));
        Path location = createTempDir();
        String translogFile = "translog-23.tlog", checkpointFile = "translog-23.ckp";
        Files.createFile(location.resolve(translogFile));
        Files.createFile(location.resolve(checkpointFile));

        TranslogTransferManager translogTransferManager = new TranslogTransferManager(transferService, remoteBaseTransferPath, tracker);

        when(transferService.downloadBlob(any(BlobPath.class), eq(translogFile))).thenReturn(
            new ByteArrayInputStream("Hello Translog".getBytes(StandardCharsets.UTF_8))
        );
        when(transferService.downloadBlob(any(BlobPath.class), eq(checkpointFile))).thenReturn(
            new ByteArrayInputStream("Hello Checkpoint".getBytes(StandardCharsets.UTF_8))
        );

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
    }

    public void testDeleteTranslogSuccess() throws Exception {
        FileTransferTracker tracker = new FileTransferTracker(new ShardId("index", "indexUuid", 0));
        BlobStore blobStore = mock(BlobStore.class);
        BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);
        BlobStoreTransferService blobStoreTransferService = new BlobStoreTransferService(blobStore, threadPool);
        TranslogTransferManager translogTransferManager = new TranslogTransferManager(
            blobStoreTransferService,
            remoteBaseTransferPath,
            tracker
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

    public void testDeleteTranslogFailure() throws Exception {
        FileTransferTracker tracker = new FileTransferTracker(new ShardId("index", "indexUuid", 0));
        BlobStore blobStore = mock(BlobStore.class);
        BlobContainer blobContainer = mock(BlobContainer.class);
        doAnswer(invocation -> { throw new IOException("test exception"); }).when(blobStore).blobContainer(any(BlobPath.class));
        // when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);
        BlobStoreTransferService blobStoreTransferService = new BlobStoreTransferService(blobStore, threadPool);
        TranslogTransferManager translogTransferManager = new TranslogTransferManager(
            blobStoreTransferService,
            remoteBaseTransferPath,
            tracker
        );
        String translogFile = "translog-19.tlog", checkpointFile = "translog-19.ckp";
        tracker.add(translogFile, true);
        tracker.add(checkpointFile, true);
        assertEquals(2, tracker.allUploaded().size());

        translogTransferManager.deleteGenerationAsync(primaryTerm, Set.of(19L), () -> {});
        assertEquals(2, tracker.allUploaded().size());
    }
}

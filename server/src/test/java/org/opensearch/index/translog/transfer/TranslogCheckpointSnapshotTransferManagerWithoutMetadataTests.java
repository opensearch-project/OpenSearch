/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class TranslogCheckpointSnapshotTransferManagerWithoutMetadataTests extends OpenSearchTestCase {

    private TransferService transferService;
    private FileTransferTracker fileTransferTracker;
    private RemoteStoreSettings remoteStoreSettings;
    private final ShardId shardId = new ShardId("index", "_na_", 1);
    private TranslogCheckpointSnapshotTransferManagerWithoutMetadata snapshotTransferManager;
    private final long primaryTerm = 2;
    private final long generation = 10;
    private final long minTranslogGeneration = 4;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        fileTransferTracker = new FileTransferTracker(shardId, new RemoteTranslogTransferTracker(shardId, 20), false);
        transferService = mock(TransferService.class);

        snapshotTransferManager = new TranslogCheckpointSnapshotTransferManagerWithoutMetadata(transferService, fileTransferTracker);

    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @AwaitsFix(bugUrl = "")
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

        // Mock fileTransferTracker to return true for all files
        for (TranslogCheckpointSnapshot snapshot : toUpload) {
            fileTransferTracker.add(snapshot.getTranslogFileName(), true);
        }

        snapshotTransferManager.transferTranslogCheckpointSnapshot(toUpload, blobPathMap, listener, WritePriority.NORMAL);
        assertEquals(successCount.get(), 2);
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

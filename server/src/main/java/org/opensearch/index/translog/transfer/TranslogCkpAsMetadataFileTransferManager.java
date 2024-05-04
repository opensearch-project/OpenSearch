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
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;
import org.opensearch.indices.RemoteStoreSettings;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Translog transfer manager to transfer {@link TranslogCheckpointSnapshot} by transfering {@link TranslogFileSnapshot} with object metadata
 *
 * @opensearch.internal
 */
public class TranslogCkpAsMetadataFileTransferManager extends TranslogTransferManager {

    TransferService transferService;

    public TranslogCkpAsMetadataFileTransferManager(
        ShardId shardId,
        TransferService transferService,
        BlobPath remoteDataTransferPath,
        BlobPath remoteMetadataTransferPath,
        FileTransferTracker fileTransferTracker,
        RemoteTranslogTransferTracker remoteTranslogTransferTracker,
        RemoteStoreSettings remoteStoreSettings
    ) {
        super(
            shardId,
            transferService,
            remoteDataTransferPath,
            remoteMetadataTransferPath,
            fileTransferTracker,
            remoteTranslogTransferTracker,
            remoteStoreSettings
        );
        this.transferService = transferService;
    }

    @Override
    public void transferTranslogCheckpointSnapshot(
        Set<TranslogCheckpointSnapshot> toUpload,
        Map<Long, BlobPath> blobPathMap,
        LatchedActionListener<TranslogCheckpointSnapshot> latchedActionListener
    ) throws Exception {
        Set<TransferFileSnapshot> filesToUpload = new HashSet<>();
        Map<TransferFileSnapshot, TranslogCheckpointSnapshot> fileToGenerationSnapshotMap = new HashMap<>();
        for (TranslogCheckpointSnapshot translogCheckpointSnapshot : toUpload) {
            TransferFileSnapshot transferFileSnapshot = translogCheckpointSnapshot.getTranslogFileSnapshotWithMetadata();
            fileToGenerationSnapshotMap.put(transferFileSnapshot, translogCheckpointSnapshot);
            filesToUpload.add(transferFileSnapshot);
        }
        ActionListener<TransferFileSnapshot> actionListener = ActionListener.wrap(
            res -> latchedActionListener.onResponse(fileToGenerationSnapshotMap.get(res)),
            ex -> {
                assert ex instanceof FileTransferException;
                FileTransferException e = (FileTransferException) ex;
                TransferFileSnapshot failedSnapshot = e.getFileSnapshot();
                latchedActionListener.onFailure(
                    new TranslogTransferException(fileToGenerationSnapshotMap.get(failedSnapshot), ex, Set.of(failedSnapshot), null)
                );
            }
        );
        transferService.uploadBlobs(filesToUpload, blobPathMap, actionListener, WritePriority.HIGH);
    }
}

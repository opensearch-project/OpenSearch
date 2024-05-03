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
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.transfer.FileSnapshot.CheckpointFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;
import org.opensearch.indices.RemoteStoreSettings;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Translog transfer manager to transfer {@link TranslogCheckpointSnapshot} by transfering {@link TranslogFileSnapshot} and {@link CheckpointFileSnapshot} separately
 *
 * @opensearch.internal
 */
public class TranslogCheckpointFilesTransferManager extends BaseTranslogTransferManager {

    TransferService transferService;
    FileTransferTracker fileTransferTracker;

    public TranslogCheckpointFilesTransferManager(
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
        this.fileTransferTracker = fileTransferTracker;
    }

    @Override
    public void transferTranslogCheckpointSnapshot(
        Set<TranslogCheckpointSnapshot> generationalSnapshotList,
        Map<Long, BlobPath> blobPathMap,
        LatchedActionListener<TranslogCheckpointSnapshot> latchedActionListener,
        WritePriority writePriority
    ) throws Exception {
        for (TranslogCheckpointSnapshot tlogAndCkpTransferFileSnapshot : generationalSnapshotList) {
            Set<TransferFileSnapshot> filesToUpload = new HashSet<>();
            Set<Exception> exceptionList = ConcurrentCollections.newConcurrentSet();

            String tlogFileName = tlogAndCkpTransferFileSnapshot.getTranslogFileName();
            String ckpFileName = tlogAndCkpTransferFileSnapshot.getCheckpointFileName();

            if (!fileTransferTracker.uploaded(tlogFileName)) {
                filesToUpload.add(tlogAndCkpTransferFileSnapshot.getTranslogFileSnapshot());
            }
            if (!fileTransferTracker.uploaded(ckpFileName)) {
                filesToUpload.add(tlogAndCkpTransferFileSnapshot.getCheckpointFileSnapshot());
            }

            assert !filesToUpload.isEmpty();

            AtomicBoolean listenerProcessed = new AtomicBoolean();
            final CountDownLatch latch = new CountDownLatch(filesToUpload.size());

            if (latch.getCount() == 0) {
                latchedActionListener.onResponse(tlogAndCkpTransferFileSnapshot);
            }

            Set<TransferFileSnapshot> succeededFiles = ConcurrentCollections.newConcurrentSet();
            Set<TransferFileSnapshot> failedFiles = new HashSet<>();

            LatchedActionListener<TransferFileSnapshot> fileUploadListener = new LatchedActionListener<>(
                ActionListener.wrap(succeededFiles::add, exceptionList::add),
                latch
            );
            ActionListener<TransferFileSnapshot> actionListener = ActionListener.runAfter(fileUploadListener, () -> {
                if (latch.getCount() == 0 && listenerProcessed.compareAndSet(false, true)) {
                    if (exceptionList.isEmpty()) {
                        assert succeededFiles.size() == filesToUpload.size();
                        latchedActionListener.onResponse(tlogAndCkpTransferFileSnapshot);
                    } else {
                        exceptionList.forEach(exception -> {
                            assert exception instanceof FileTransferException;
                            FileTransferException fileTransferException = (FileTransferException) exception;
                            TransferFileSnapshot transferFileSnapshot = fileTransferException.getFileSnapshot();
                            failedFiles.add(transferFileSnapshot);
                        });
                        Exception ex = new TranslogUploadFailedException("Translog upload failed");
                        exceptionList.forEach(ex::addSuppressed);
                        latchedActionListener.onFailure(
                            new TranslogTransferException(tlogAndCkpTransferFileSnapshot, ex, failedFiles, succeededFiles)
                        );
                    }
                }
            });
            transferService.uploadBlobs(filesToUpload, blobPathMap, actionListener, writePriority);
        }
    }
}

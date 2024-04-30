/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.logging.Loggers;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.translog.transfer.FileSnapshot.CheckpointFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;
import org.opensearch.indices.RemoteStoreSettings;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Translog transfer manager to transfer {@link TranslogCheckpointSnapshot} by transfering {@link TranslogFileSnapshot} and {@link CheckpointFileSnapshot} separately
 *
 * @opensearch.internal
 */
public class TranslogCheckpointSnapshotTransferManagerWithoutMetadata implements TranslogCheckpointSnapshotTransferManager {

    private final TransferService transferService;
    private final FileTransferTracker fileTransferTracker;
    private final RemoteStoreSettings remoteStoreSettings;
    private final ShardId shardId;
    private final Logger logger;

    public TranslogCheckpointSnapshotTransferManagerWithoutMetadata(
        TransferService transferService,
        FileTransferTracker fileTransferTracker,
        RemoteStoreSettings remoteStoreSettings,
        ShardId shardId
    ) {
        this.transferService = transferService;
        this.fileTransferTracker = fileTransferTracker;
        this.remoteStoreSettings = remoteStoreSettings;
        this.shardId = shardId;
        this.logger = Loggers.getLogger(getClass(), shardId);
    }

    @Override
    public void transferTranslogCheckpointSnapshot(
        TransferSnapshot transferSnapshot,
        Set<TranslogCheckpointSnapshot> toUpload,
        Map<Long, BlobPath> blobPathMap,
        LatchedActionListener<TranslogCheckpointSnapshot> latchedActionListener,
        WritePriority writePriority
    ) throws Exception {
        for (TranslogCheckpointSnapshot tlogAndCkpTransferFileSnapshot : toUpload) {
            Set<FileSnapshot.TransferFileSnapshot> filesToUpload = new HashSet<>(2);
            Set<Exception> exceptionList = new HashSet<>(2);

            TransferFileSnapshot checkpointFileSnapshot = tlogAndCkpTransferFileSnapshot.getCheckpointFileSnapshot();
            TransferFileSnapshot translogFileSnapshot = tlogAndCkpTransferFileSnapshot.getTranslogFileSnapshot();
            String tlogFileName = translogFileSnapshot.getName();
            String ckpFileName = checkpointFileSnapshot.getName();

            if (!fileTransferTracker.uploaded(tlogFileName)) {
                filesToUpload.add(translogFileSnapshot);
            }
            if (!fileTransferTracker.uploaded(ckpFileName)) {
                filesToUpload.add(checkpointFileSnapshot);
            }
            if (filesToUpload.isEmpty()) {
                logger.info("Nothing to upload for transfer");
                latchedActionListener.onResponse(tlogAndCkpTransferFileSnapshot);
                return;
            }
            final CountDownLatch latch = new CountDownLatch(filesToUpload.size());
            LatchedActionListener<TransferFileSnapshot> onCompletionLatchedActionListener = new LatchedActionListener<>(
                ActionListener.wrap(fileSnapshot -> fileTransferTracker.add(fileSnapshot.getName(), true), ex -> {
                    assert ex instanceof FileTransferException;
                    logger.error(
                        () -> new ParameterizedMessage(
                            "Exception during transfer for file {}",
                            ((FileTransferException) ex).getFileSnapshot().getName()
                        ),
                        ex
                    );
                    FileTransferException e = (FileTransferException) ex;
                    FileSnapshot.TransferFileSnapshot file = e.getFileSnapshot();
                    fileTransferTracker.add(file.getName(), false);
                    exceptionList.add(ex);
                }),
                latch
            );

            transferService.uploadBlobs(filesToUpload, blobPathMap, onCompletionLatchedActionListener, writePriority);

            try {
                if (!latch.await(remoteStoreSettings.getClusterRemoteTranslogTransferTimeout().millis(), TimeUnit.MILLISECONDS)) {
                    Exception ex = new TranslogUploadFailedException(
                        "Timed out waiting for transfer of snapshot " + transferSnapshot + " to complete"
                    );
                    exceptionList.forEach(ex::addSuppressed);
                    Exception exception = new TranslogGenerationTransferException(tlogAndCkpTransferFileSnapshot, ex);
                    latchedActionListener.onFailure(exception);
                    return;
                }
            } catch (InterruptedException e) {
                Exception exception = new TranslogUploadFailedException("Failed to upload " + transferSnapshot, e);
                exceptionList.forEach(exception::addSuppressed);
                Thread.currentThread().interrupt();
                Exception ex = new TranslogGenerationTransferException(tlogAndCkpTransferFileSnapshot, exception);
                latchedActionListener.onFailure(ex);
                return;
            }

            if (exceptionList.isEmpty()) {
                latchedActionListener.onResponse(tlogAndCkpTransferFileSnapshot);
            } else {
                Exception ex = new TranslogUploadFailedException("Failed to upload snapshot " + tlogAndCkpTransferFileSnapshot);
                exceptionList.forEach(ex::addSuppressed);
                Exception exception = new TranslogGenerationTransferException(tlogAndCkpTransferFileSnapshot, ex);
                latchedActionListener.onFailure(exception);
                return;
            }
        }
    }
}

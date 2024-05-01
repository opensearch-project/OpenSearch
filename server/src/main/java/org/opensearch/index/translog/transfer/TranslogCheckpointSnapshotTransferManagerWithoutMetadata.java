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
import org.opensearch.index.translog.transfer.FileSnapshot.CheckpointFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;

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
public class TranslogCheckpointSnapshotTransferManagerWithoutMetadata implements TranslogCheckpointSnapshotTransferManager {

    private final TransferService transferService;
    private final FileTransferTracker fileTransferTracker;

    public TranslogCheckpointSnapshotTransferManagerWithoutMetadata(
        TransferService transferService,
        FileTransferTracker fileTransferTracker
    ) {
        this.transferService = transferService;
        this.fileTransferTracker = fileTransferTracker;
    }

    @Override
    public void transferTranslogCheckpointSnapshot(
        Set<TranslogCheckpointSnapshot> toUpload,
        Map<Long, BlobPath> blobPathMap,
        LatchedActionListener<TranslogCheckpointSnapshot> latchedActionListener,
        WritePriority writePriority
    ) throws Exception {
        for (TranslogCheckpointSnapshot tlogAndCkpTransferFileSnapshot : toUpload) {
            Set<TransferFileSnapshot> filesToUpload = new HashSet<>(2);
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

            assert !filesToUpload.isEmpty();

            AtomicBoolean atomicBoolean = new AtomicBoolean();
            final CountDownLatch latch = new CountDownLatch(filesToUpload.size());

            Set<TransferFileSnapshot> successFiles = new HashSet<>();
            Set<TransferFileSnapshot> failedFiles = new HashSet<>();

            LatchedActionListener<TransferFileSnapshot> fileUploadListener = new LatchedActionListener<>(
                ActionListener.wrap(successFiles::add, exceptionList::add),
                latch
            );
            ActionListener<TransferFileSnapshot> actionListener = ActionListener.runAfter(fileUploadListener, () -> {
                if (latch.getCount() == 0 && atomicBoolean.compareAndSet(false, true)) {
                    if (exceptionList.isEmpty()) {
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
                            new TranslogGenerationTransferException(tlogAndCkpTransferFileSnapshot, ex, failedFiles, successFiles)
                        );
                    }
                }
            });
            transferService.uploadBlobs(filesToUpload, blobPathMap, actionListener, writePriority);
        }
    }
}

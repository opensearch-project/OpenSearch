/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.index.translog.FileSnapshot;
import org.opensearch.index.translog.transfer.listener.FileTransferListener;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.UnaryOperator;

/**
 * The class responsible for orchestrating the transfer via a {@link TransferService}
 */
public class TranslogTransferManager {

    private final TransferService transferService;
    private final Iterable<String> remoteTransferPath;
    private final FileTransferListener fileTransferListener;
    private final UnaryOperator<Set<FileSnapshot>> exclusionFilter;
    private static final long TRANSFER_TIMEOUT = 10;

    private static final Logger logger = LogManager.getLogger(TranslogTransferManager.class);

    public TranslogTransferManager(
        TransferService transferService,
        Iterable<String> remoteTransferPath,
        FileTransferListener fileTransferListener,
        UnaryOperator<Set<FileSnapshot>> exclusionFilter
    ) {
        this.transferService = transferService;
        this.remoteTransferPath = remoteTransferPath;
        this.fileTransferListener = fileTransferListener;
        this.exclusionFilter = exclusionFilter;
    }

    public boolean uploadTranslog(TransferSnapshot translogCheckpointTransferSnapshot, TranslogTransferListener translogTransferListener)
        throws IOException {
        List<Exception> exceptionList = new ArrayList<>(translogCheckpointTransferSnapshot.getTransferSize());
        try {
            Set<FileSnapshot> toUpload = exclusionFilter.apply(translogCheckpointTransferSnapshot.getTranslogFileSnapshots());
            toUpload.addAll(exclusionFilter.apply(translogCheckpointTransferSnapshot.getCheckpointFileSnapshots()));
            if (toUpload.isEmpty()) {
                logger.warn("Nothing to upload for transfer size {}", translogCheckpointTransferSnapshot.getTransferSize());
                return true;
            }
            final CountDownLatch latch = new CountDownLatch(toUpload.size());
            LatchedActionListener<FileSnapshot> latchedActionListener = new LatchedActionListener(
                ActionListener.wrap(fileTransferListener::onSuccess, ex -> {
                    assert ex instanceof FileTransferException;
                    logger.error("Exception received type {}", ex.getClass(), ex);
                    FileTransferException e = (FileTransferException) ex;
                    fileTransferListener.onFailure(e.getFileSnapshot(), ex);
                    exceptionList.add(ex);
                }),
                latch
            );
            toUpload.forEach(fileSnapshot -> transferService.uploadFile(fileSnapshot, remoteTransferPath, latchedActionListener));
            try {
                if (latch.await(TRANSFER_TIMEOUT, TimeUnit.SECONDS) == false) {
                    exceptionList.add(new TimeoutException("Timed out waiting for transfer to complete"));
                }
            } catch (InterruptedException ex) {
                logger.error(() -> new ParameterizedMessage("Time failed for snapshot {}", translogCheckpointTransferSnapshot), ex);
                exceptionList.add(ex);
                Thread.currentThread().interrupt();
            }
            if (exceptionList.isEmpty()) {
                translogTransferListener.onUploadComplete(translogCheckpointTransferSnapshot);
            } else {
                translogTransferListener.onUploadFailed(translogCheckpointTransferSnapshot, ExceptionsHelper.multiple(exceptionList));
            }
            return exceptionList.isEmpty();
        } catch (Exception ex) {
            logger.error(() -> new ParameterizedMessage("Transfer failed for snapshot {}", translogCheckpointTransferSnapshot), ex);
            translogTransferListener.onUploadFailed(translogCheckpointTransferSnapshot, ex);
            return false;
        }
    }
}

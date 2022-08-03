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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The class responsible for orchestrating the transfer via a {@link TransferService}
 */
public class TranslogTransferManager {

    private final TransferService transferService;
    private final RemotePathProvider remotePathProvider;
    private final TranslogTransferListener translogTransferListener;
    private final FileTransferListener fileTransferListener;
    private static final long TRANSFER_TIMEOUT = 10;

    private static final Logger logger = LogManager.getLogger(TranslogTransferManager.class);

    public TranslogTransferManager(
        TransferService transferService,
        RemotePathProvider remotePathProvider,
        TranslogTransferListener translogTransferListener,
        FileTransferListener fileTransferListener
    ) {
        this.transferService = transferService;
        this.remotePathProvider = remotePathProvider;
        this.translogTransferListener = translogTransferListener;
        this.fileTransferListener = fileTransferListener;
    }

    boolean uploadTranslog(TransferSnapshotProvider transferSnapshotProvider) {
        final TransferSnapshotProvider.TranslogCheckpointTransferSnapshot translogCheckpointTransferSnapshot = transferSnapshotProvider
            .get();
        List<Exception> exceptionList = new ArrayList<>(translogCheckpointTransferSnapshot.getTransferSize());
        try {
            final CountDownLatch latch = new CountDownLatch(translogCheckpointTransferSnapshot.getTransferSize());
            LatchedActionListener<FileSnapshot> latchedActionListener = new LatchedActionListener(
                ActionListener.wrap(fileTransferListener::onSuccess, ex -> {
                    assert ex instanceof FileTransferException;
                    FileTransferException e = (FileTransferException) ex;
                    fileTransferListener.onFailure(e.getFileSnapshot(), ex);
                    exceptionList.add(ex);
                }),
                latch
            );
            translogCheckpointTransferSnapshot.getTranslogFileSnapshots()
                .forEach(fileSnapshot -> transferService.uploadFile(fileSnapshot, remotePathProvider, latchedActionListener));
            translogCheckpointTransferSnapshot.getCheckpointFileSnapshots()
                .forEach(fileSnapshot -> transferService.uploadFile(fileSnapshot, remotePathProvider, latchedActionListener));

            latch.await(TRANSFER_TIMEOUT, TimeUnit.SECONDS);

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

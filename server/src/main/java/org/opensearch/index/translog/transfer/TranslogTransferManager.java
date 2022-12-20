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
import org.opensearch.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.translog.transfer.listener.FileTransferListener;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import static org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;

/**
 * The class responsible for orchestrating the transfer of a {@link TransferSnapshot} via a {@link TransferService}
 *
 * @opensearch.internal
 */
public class TranslogTransferManager {

    private final TransferService transferService;
    private final BlobPath remoteBaseTransferPath;
    private final BlobPath remoteMetadaTransferPath;
    private final FileTransferListener fileTransferListener;
    private final UnaryOperator<Set<TransferFileSnapshot>> exclusionFilter;

    private static final long TRANSFER_TIMEOUT_IN_MILLIS = 30000;

    private static final Logger logger = LogManager.getLogger(TranslogTransferManager.class);

    private final static String METADATA_DIR = "metadata";

    public TranslogTransferManager(
        TransferService transferService,
        BlobPath remoteBaseTransferPath,
        FileTransferListener fileTransferListener,
        UnaryOperator<Set<TransferFileSnapshot>> exclusionFilter
    ) {
        this.transferService = transferService;
        this.remoteBaseTransferPath = remoteBaseTransferPath;
        this.remoteMetadaTransferPath = remoteBaseTransferPath.add(METADATA_DIR);
        this.fileTransferListener = fileTransferListener;
        this.exclusionFilter = exclusionFilter;
    }

    public boolean transferSnapshot(TransferSnapshot transferSnapshot, TranslogTransferListener translogTransferListener)
        throws IOException {
        List<Exception> exceptionList = new ArrayList<>(transferSnapshot.getTranslogTransferMetadata().getCount());
        Set<TransferFileSnapshot> toUpload = new HashSet<>(transferSnapshot.getTranslogTransferMetadata().getCount());
        try {
            toUpload.addAll(exclusionFilter.apply(transferSnapshot.getTranslogFileSnapshots()));
            toUpload.addAll(exclusionFilter.apply(transferSnapshot.getCheckpointFileSnapshots()));
            if (toUpload.isEmpty()) {
                logger.trace("Nothing to upload for transfer");
                translogTransferListener.onUploadComplete(transferSnapshot);
                return true;
            }
            final CountDownLatch latch = new CountDownLatch(toUpload.size());
            LatchedActionListener<TransferFileSnapshot> latchedActionListener = new LatchedActionListener<>(
                ActionListener.wrap(fileTransferListener::onSuccess, ex -> {
                    assert ex instanceof FileTransferException;
                    logger.error(
                        () -> new ParameterizedMessage(
                            "Exception during transfer for file {}",
                            ((FileTransferException) ex).getFileSnapshot().getName()
                        ),
                        ex
                    );
                    FileTransferException e = (FileTransferException) ex;
                    fileTransferListener.onFailure(e.getFileSnapshot(), ex);
                    exceptionList.add(ex);
                }),
                latch
            );
            toUpload.forEach(
                fileSnapshot -> transferService.uploadBlobAsync(
                    fileSnapshot,
                    remoteBaseTransferPath.add(String.valueOf(fileSnapshot.getPrimaryTerm())),
                    latchedActionListener
                )
            );
            try {
                if (latch.await(TRANSFER_TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS) == false) {
                    Exception ex = new TimeoutException("Timed out waiting for transfer of snapshot " + transferSnapshot + " to complete");
                    exceptionList.forEach(ex::addSuppressed);
                    throw ex;
                }
            } catch (InterruptedException ex) {
                exceptionList.forEach(ex::addSuppressed);
                Thread.currentThread().interrupt();
                throw ex;
            }
            if (exceptionList.isEmpty()) {
                transferService.uploadBlob(prepareMetadata(transferSnapshot), remoteMetadaTransferPath);
                translogTransferListener.onUploadComplete(transferSnapshot);
                return true;
            } else {
                Exception ex = new IOException("Failed to upload " + exceptionList.size() + " files during transfer");
                exceptionList.forEach(ex::addSuppressed);
                throw ex;
            }
        } catch (Exception ex) {
            logger.error(() -> new ParameterizedMessage("Transfer failed for snapshot {}", transferSnapshot), ex);
            translogTransferListener.onUploadFailed(transferSnapshot, ex);
            return false;
        }
    }

    private TransferFileSnapshot prepareMetadata(TransferSnapshot transferSnapshot) throws IOException {
        Map<String, String> generationPrimaryTermMap = transferSnapshot.getTranslogFileSnapshots().stream().map(s -> {
            assert s instanceof TranslogFileSnapshot;
            return (TranslogFileSnapshot) s;
        })
            .collect(
                Collectors.toMap(
                    snapshot -> String.valueOf(snapshot.getGeneration()),
                    snapshot -> String.valueOf(snapshot.getPrimaryTerm())
                )
            );
        TranslogTransferMetadata translogTransferMetadata = transferSnapshot.getTranslogTransferMetadata();
        translogTransferMetadata.setGenerationToPrimaryTermMapper(new HashMap<>(generationPrimaryTermMap));
        TransferFileSnapshot fileSnapshot = new TransferFileSnapshot(
            translogTransferMetadata.getFileName(),
            translogTransferMetadata.createMetadataBytes(),
            translogTransferMetadata.getPrimaryTerm()
        );

        return fileSnapshot;
    }
}

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
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.index.translog.FileSnapshot;
import org.opensearch.index.translog.RemoteTranslogMetadata;
import org.opensearch.index.translog.transfer.listener.FileTransferListener;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import static org.opensearch.index.translog.FileSnapshot.TranslogFileSnapshot;

/**
 * The class responsible for orchestrating the transfer via a {@link TransferService}
 */
public class TranslogTransferManager {

    private final TransferService transferService;
    private final BlobPath remoteBaseTransferPath;
    private final BlobPath remoteTransferMetadataPath;
    private final FileTransferListener fileTransferListener;
    private final UnaryOperator<Set<FileSnapshot>> exclusionFilter;
    private static final long TRANSFER_TIMEOUT = 10;

    private static final Logger logger = LogManager.getLogger(TranslogTransferManager.class);

    public TranslogTransferManager(
        TransferService transferService,
        BlobPath remoteBaseTransferPath,
        BlobPath remoteTransferMetadataPath,
        FileTransferListener fileTransferListener,
        UnaryOperator<Set<FileSnapshot>> exclusionFilter
    ) {
        this.transferService = transferService;
        this.remoteBaseTransferPath = remoteBaseTransferPath;
        this.remoteTransferMetadataPath = remoteTransferMetadataPath;
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
            toUpload.forEach(
                fileSnapshot -> transferService.uploadFileAsync(
                    fileSnapshot,
                    remoteBaseTransferPath.add(String.valueOf(fileSnapshot.getPrimaryTerm())),
                    latchedActionListener
                )
            );
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
                transferService.uploadFile(prepareMetadata(translogCheckpointTransferSnapshot), remoteTransferMetadataPath);
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

    private FileSnapshot prepareMetadata(TransferSnapshot transferSnapshot) throws IOException {
        RemoteTranslogMetadata remoteTranslogMetadata = new RemoteTranslogMetadata(
            transferSnapshot.getPrimaryTerm(),
            transferSnapshot.getGeneration(),
            transferSnapshot.getMinGeneration()
        );
        assert transferSnapshot.getTranslogFileSnapshots() instanceof TranslogFileSnapshot;
        Map<String, String> generationPrimaryTermMap = transferSnapshot.getTranslogFileSnapshots().stream().map(s -> {
            assert s instanceof TranslogFileSnapshot;
            return (TranslogFileSnapshot) s;
        })
            .collect(
                Collectors.toMap(
                    snapshot -> Long.toString(snapshot.getGeneration(), Character.MAX_RADIX),
                    snapshot -> Long.toString(snapshot.getPrimaryTerm(), Character.MAX_RADIX)
                )
            );
        remoteTranslogMetadata.setGenerationToPrimaryTermMapper(new HashMap<>(generationPrimaryTermMap));
        FileSnapshot fileSnapshot;
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            remoteTranslogMetadata.writeTo(output);
            try (
                CheckedInputStream stream = new CheckedInputStream(
                    new ByteArrayInputStream(output.bytes().streamInput().readByteArray()),
                    new CRC32()
                )
            ) {
                byte[] content = stream.readAllBytes();
                long checksum = stream.getChecksum().getValue();
                fileSnapshot = new FileSnapshot(remoteTranslogMetadata.getMetadataFileName(), checksum, content);
            }
        }
        return fileSnapshot;
    }
}

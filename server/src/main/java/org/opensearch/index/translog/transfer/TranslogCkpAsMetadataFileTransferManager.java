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
import org.opensearch.common.blobstore.InputStreamWithMetadata;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;
import org.opensearch.indices.RemoteStoreSettings;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Translog transfer manager to transfer {@link TranslogCheckpointSnapshot} by transfering {@link TranslogFileSnapshot} with object metadata
 *
 * @opensearch.internal
 */
public class TranslogCkpAsMetadataFileTransferManager extends TranslogTransferManager {

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
                    new TranslogTransferException(
                        fileToGenerationSnapshotMap.get(failedSnapshot),
                        ex,
                        Set.of(failedSnapshot),
                        Collections.emptySet()
                    )
                );
            }
        );
        transferService.uploadBlobs(filesToUpload, blobPathMap, actionListener, WritePriority.HIGH);
    }

    @Override
    public boolean downloadTranslog(String primaryTerm, String generation, Path location) throws IOException {
        logger.trace(
            "Downloading translog file with object metadata: Primary Term = {}, Generation = {}, Location = {}",
            primaryTerm,
            generation,
            location
        );
        // Download translog.tlog file with object metadata from remote to local FS
        String translogFilename = Translog.getFilename(Long.parseLong(generation));
        Map<String, String> metadata = downloadTranslogToFSAndGetMetadata(translogFilename, location, primaryTerm, generation);
        try {
            assert metadata != null && !metadata.isEmpty() && metadata.containsKey(CHECKPOINT_FILE_DATA_KEY);
            recoverCkpFileFromMetadata(metadata, location, generation, translogFilename);
        } catch (Exception e) {
            throw new IOException("Failed to recover checkpoint file from remote", e);
        }
        // Mark in FileTransferTracker that translog files for the generation is downloaded from remote
        fileTransferTracker.addGeneration(Long.parseLong(generation), true);
        return true;
    }

    private Map<String, String> downloadTranslogToFSAndGetMetadata(String fileName, Path location, String primaryTerm, String generation)
        throws IOException {
        Path filePath = location.resolve(fileName);
        // Here, we always override the existing file if present.
        // We need to change this logic when we introduce incremental download
        deleteFileIfExists(filePath);

        boolean downloadStatus = false;
        long bytesToRead = 0, downloadStartTime = System.nanoTime();
        Map<String, String> metadata;

        try (
            InputStreamWithMetadata inputStreamWithMetadata = transferService.downloadBlobWithMetadata(
                remoteDataTransferPath.add(primaryTerm),
                fileName
            )
        ) {
            InputStream inputStream = inputStreamWithMetadata.getInputStream();
            metadata = inputStreamWithMetadata.getMetadata();

            bytesToRead = inputStream.available();
            Files.copy(inputStream, filePath);
            downloadStatus = true;

        } finally {
            remoteTranslogTransferTracker.addDownloadTimeInMillis((System.nanoTime() - downloadStartTime) / 1_000_000L);
            if (downloadStatus) {
                remoteTranslogTransferTracker.addDownloadBytesSucceeded(bytesToRead);
            }
        }

        return metadata;
    }

    /**
     * Process the provided metadata and tries to write the content of the checkpoint (ckp) file to the FS.
     */
    private void recoverCkpFileFromMetadata(Map<String, String> metadata, Path location, String generation, String fileName)
        throws IOException {

        boolean downloadStatus = false;
        long bytesToRead = 0;
        try {
            String ckpFileName = Translog.getCommitCheckpointFileName(Long.parseLong(generation));
            Path filePath = location.resolve(ckpFileName);
            // Here, we always override the existing file if present.
            deleteFileIfExists(filePath);

            String ckpDataBase64 = metadata.get(CHECKPOINT_FILE_DATA_KEY);
            if (ckpDataBase64 == null) {
                logger.error("Error processing metadata for translog file: {}", fileName);
                throw new IllegalStateException(
                    "Checkpoint file data (key - ckp-data) is expected but not found in metadata for file: " + fileName
                );
            }
            byte[] ckpFileBytes = Base64.getDecoder().decode(ckpDataBase64);
            bytesToRead = ckpFileBytes.length;

            Files.write(filePath, ckpFileBytes);
            downloadStatus = true;
        } finally {
            if (downloadStatus) {
                remoteTranslogTransferTracker.addDownloadBytesSucceeded(bytesToRead);
            }
        }
    }

    @Override
    public void deleteGenerationAsync(long primaryTerm, Set<Long> generations, Runnable onCompletion) {
        List<String> translogFiles = new ArrayList<>();
        generations.forEach(generation -> {
            // Add .ckp and .tlog file to translog file list which is located in basePath/<primaryTerm>
            String translogFileName = Translog.getFilename(generation);
            translogFiles.add(translogFileName);
        });
        // Delete the translog and checkpoint files asynchronously
        deleteTranslogFilesAsync(primaryTerm, translogFiles, onCompletion, generations);
    }

}

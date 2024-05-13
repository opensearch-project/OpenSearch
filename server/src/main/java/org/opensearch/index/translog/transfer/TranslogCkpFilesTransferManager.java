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
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.transfer.FileSnapshot.CheckpointFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;
import org.opensearch.indices.RemoteStoreSettings;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Translog transfer manager to transfer {@link TranslogCheckpointSnapshot} by transfering {@link TranslogFileSnapshot} and {@link CheckpointFileSnapshot} separately
 *
 * @opensearch.internal
 */
public class TranslogCkpFilesTransferManager extends TranslogTransferManager {

    public TranslogCkpFilesTransferManager(
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
        for (TranslogCheckpointSnapshot tlogAndCkpTransferFileSnapshot : toUpload) {
            Set<TransferFileSnapshot> filesToUpload = new HashSet<>();
            String tlogFileName = tlogAndCkpTransferFileSnapshot.getTranslogFileName();
            String ckpFileName = tlogAndCkpTransferFileSnapshot.getCheckpointFileName();

            assert fileTransferTracker instanceof TranslogCkpFilesTransferTracker;
            TranslogCkpFilesTransferTracker translogCkpFilesTransferTracker = (TranslogCkpFilesTransferTracker) fileTransferTracker;
            if (translogCkpFilesTransferTracker.uploaded(tlogFileName) == false) {
                filesToUpload.add(tlogAndCkpTransferFileSnapshot.getTranslogFileSnapshot());
            }
            if (translogCkpFilesTransferTracker.uploaded(ckpFileName) == false) {
                filesToUpload.add(tlogAndCkpTransferFileSnapshot.getCheckpointFileSnapshot());
            }
            if (filesToUpload.isEmpty()) {
                logger.info("Returning from transferTranslogCheckpointSnapshot without any upload");
                latchedActionListener.onResponse(tlogAndCkpTransferFileSnapshot);
            }

            final CountDownLatch latch = new CountDownLatch(filesToUpload.size());
            Set<TransferFileSnapshot> succeededFiles = ConcurrentCollections.newConcurrentSet();
            Set<TransferFileSnapshot> failedFiles = new HashSet<>();
            Set<Exception> exceptionList = ConcurrentCollections.newConcurrentSet();
            LatchedActionListener<TransferFileSnapshot> fileUploadListener = new LatchedActionListener<>(
                ActionListener.wrap(succeededFiles::add, exceptionList::add),
                latch
            );
            AtomicBoolean listenerProcessed = new AtomicBoolean();
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
            transferService.uploadBlobs(filesToUpload, blobPathMap, actionListener, WritePriority.HIGH);
        }
    }

    @Override
    public boolean downloadTranslog(String primaryTerm, String generation, Path location) throws IOException {
        logger.trace(
            "Downloading translog and checkpoint files with: Primary Term = {}, Generation = {}, Location = {}",
            primaryTerm,
            generation,
            location
        );
        // Download translog.tlog and translog.ckp files from remote to local FS
        String translogFilename = Translog.getFilename(Long.parseLong(generation));
        String ckpFileName = Translog.getCommitCheckpointFileName(Long.parseLong(generation));
        downloadFileToFS(translogFilename, location, primaryTerm);
        downloadFileToFS(ckpFileName, location, primaryTerm);
        fileTransferTracker.addGeneration(Long.parseLong(generation), true);
        return true;
    }

    /**
     * Downloads the translog (tlog) or checkpoint (ckp) file from a remote source and saves it to the local FS.
     *
     * @param fileName     The name of the checkpoint file (e.g., "translog.ckp").
     * @param location     The local file system path where the checkpoint file will be stored.
     * @param primaryTerm  The primary term associated with the checkpoint file.
     * @throws IOException If an I/O error occurs during the file download or copy operation.
     */
    void downloadFileToFS(String fileName, Path location, String primaryTerm) throws IOException {
        Path filePath = location.resolve(fileName);
        // Here, we always override the existing file if present.
        deleteFileIfExists(filePath);

        boolean downloadStatus = false;
        long bytesToRead = 0, downloadStartTime = System.nanoTime();
        try (InputStream inputStream = transferService.downloadBlob(remoteDataTransferPath.add(primaryTerm), fileName)) {
            // Capture number of bytes for stats before reading
            bytesToRead = inputStream.available();
            Files.copy(inputStream, filePath);
            downloadStatus = true;
        } finally {
            remoteTranslogTransferTracker.addDownloadTimeInMillis((System.nanoTime() - downloadStartTime) / 1_000_000L);
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
            String ckpFileName = Translog.getCommitCheckpointFileName(generation);
            String translogFileName = Translog.getFilename(generation);
            translogFiles.add(ckpFileName);
            translogFiles.add(translogFileName);
        });
        // Delete the translog and checkpoint files asynchronously
        deleteTranslogFilesAsync(primaryTerm, translogFiles, onCompletion, generations);
    }

}

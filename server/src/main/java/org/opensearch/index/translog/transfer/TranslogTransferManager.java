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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.SetOnce;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.io.VersionedCodecStreamWrapper;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import static org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;

/**
 * The class responsible for orchestrating the transfer of a {@link TransferSnapshot} via a {@link TransferService}
 *
 * @opensearch.internal
 */
public class TranslogTransferManager {

    private final ShardId shardId;
    private final TransferService transferService;
    private final BlobPath remoteDataTransferPath;
    private final BlobPath remoteMetadataTransferPath;
    private final FileTransferTracker fileTransferTracker;
    private final RemoteTranslogTransferTracker remoteTranslogTransferTracker;

    private static final long TRANSFER_TIMEOUT_IN_MILLIS = 30000;

    private static final int METADATA_FILES_TO_FETCH = 10;

    private final Logger logger;

    private static final VersionedCodecStreamWrapper<TranslogTransferMetadata> metadataStreamWrapper = new VersionedCodecStreamWrapper<>(
        new TranslogTransferMetadataHandler(),
        TranslogTransferMetadata.CURRENT_VERSION,
        TranslogTransferMetadata.METADATA_CODEC
    );

    public TranslogTransferManager(
        ShardId shardId,
        TransferService transferService,
        BlobPath remoteDataTransferPath,
        BlobPath remoteMetadataTransferPath,
        FileTransferTracker fileTransferTracker,
        RemoteTranslogTransferTracker remoteTranslogTransferTracker
    ) {
        this.shardId = shardId;
        this.transferService = transferService;
        this.remoteDataTransferPath = remoteDataTransferPath;
        this.remoteMetadataTransferPath = remoteMetadataTransferPath;
        this.fileTransferTracker = fileTransferTracker;
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.remoteTranslogTransferTracker = remoteTranslogTransferTracker;
    }

    public RemoteTranslogTransferTracker getRemoteTranslogTransferTracker() {
        return remoteTranslogTransferTracker;
    }

    public ShardId getShardId() {
        return this.shardId;
    }

    public boolean transferSnapshot(TransferSnapshot transferSnapshot, TranslogTransferListener translogTransferListener)
        throws IOException {
        List<Exception> exceptionList = new ArrayList<>(transferSnapshot.getTranslogTransferMetadata().getCount());
        Set<TransferFileSnapshot> toUpload = new HashSet<>(transferSnapshot.getTranslogTransferMetadata().getCount());
        long metadataBytesToUpload;
        long metadataUploadStartTime;
        long uploadStartTime;
        long prevUploadBytesSucceeded = remoteTranslogTransferTracker.getUploadBytesSucceeded();
        long prevUploadTimeInMillis = remoteTranslogTransferTracker.getTotalUploadTimeInMillis();

        try {
            toUpload.addAll(fileTransferTracker.exclusionFilter(transferSnapshot.getTranslogFileSnapshots()));
            toUpload.addAll(fileTransferTracker.exclusionFilter((transferSnapshot.getCheckpointFileSnapshots())));
            if (toUpload.isEmpty()) {
                logger.trace("Nothing to upload for transfer");
                return true;
            }

            fileTransferTracker.recordBytesForFiles(toUpload);
            captureStatsBeforeUpload();
            final CountDownLatch latch = new CountDownLatch(toUpload.size());
            LatchedActionListener<TransferFileSnapshot> latchedActionListener = new LatchedActionListener<>(
                ActionListener.wrap(fileTransferTracker::onSuccess, ex -> {
                    assert ex instanceof FileTransferException;
                    logger.error(
                        () -> new ParameterizedMessage(
                            "Exception during transfer for file {}",
                            ((FileTransferException) ex).getFileSnapshot().getName()
                        ),
                        ex
                    );
                    FileTransferException e = (FileTransferException) ex;
                    TransferFileSnapshot file = e.getFileSnapshot();
                    fileTransferTracker.onFailure(file, ex);
                    exceptionList.add(ex);
                }),
                latch
            );
            Map<Long, BlobPath> blobPathMap = new HashMap<>();
            toUpload.forEach(
                fileSnapshot -> blobPathMap.put(
                    fileSnapshot.getPrimaryTerm(),
                    remoteDataTransferPath.add(String.valueOf(fileSnapshot.getPrimaryTerm()))
                )
            );

            uploadStartTime = System.nanoTime();
            // TODO: Ideally each file's upload start time should be when it is actually picked for upload
            // https://github.com/opensearch-project/OpenSearch/issues/9729
            fileTransferTracker.recordFileTransferStartTime(uploadStartTime);
            transferService.uploadBlobs(toUpload, blobPathMap, latchedActionListener, WritePriority.HIGH);

            try {
                if (latch.await(TRANSFER_TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS) == false) {
                    Exception ex = new TranslogUploadFailedException(
                        "Timed out waiting for transfer of snapshot " + transferSnapshot + " to complete"
                    );
                    exceptionList.forEach(ex::addSuppressed);
                    throw ex;
                }
            } catch (InterruptedException ex) {
                Exception exception = new TranslogUploadFailedException("Failed to upload " + transferSnapshot, ex);
                exceptionList.forEach(exception::addSuppressed);
                Thread.currentThread().interrupt();
                throw exception;
            }
            if (exceptionList.isEmpty()) {
                TransferFileSnapshot tlogMetadata = prepareMetadata(transferSnapshot);
                metadataBytesToUpload = tlogMetadata.getContentLength();
                remoteTranslogTransferTracker.addUploadBytesStarted(metadataBytesToUpload);
                metadataUploadStartTime = System.nanoTime();
                try {
                    transferService.uploadBlob(tlogMetadata, remoteMetadataTransferPath, WritePriority.HIGH);
                } catch (Exception exception) {
                    remoteTranslogTransferTracker.addUploadTimeInMillis((System.nanoTime() - metadataUploadStartTime) / 1_000_000L);
                    remoteTranslogTransferTracker.addUploadBytesFailed(metadataBytesToUpload);
                    // outer catch handles capturing stats on upload failure
                    throw new TranslogUploadFailedException("Failed to upload " + tlogMetadata.getName(), exception);
                }

                remoteTranslogTransferTracker.addUploadTimeInMillis((System.nanoTime() - metadataUploadStartTime) / 1_000_000L);
                remoteTranslogTransferTracker.addUploadBytesSucceeded(metadataBytesToUpload);
                captureStatsOnUploadSuccess(prevUploadBytesSucceeded, prevUploadTimeInMillis);
                translogTransferListener.onUploadComplete(transferSnapshot);
                return true;
            } else {
                Exception ex = new TranslogUploadFailedException("Failed to upload " + exceptionList.size() + " files during transfer");
                exceptionList.forEach(ex::addSuppressed);
                throw ex;
            }
        } catch (Exception ex) {
            logger.error(() -> new ParameterizedMessage("Transfer failed for snapshot {}", transferSnapshot), ex);
            captureStatsOnUploadFailure();
            translogTransferListener.onUploadFailed(transferSnapshot, ex);
            return false;
        }
    }

    /**
     * Adds relevant stats to the tracker when an upload is started
     */
    private void captureStatsBeforeUpload() {
        remoteTranslogTransferTracker.incrementTotalUploadsStarted();
        // TODO: Ideally each file's byte uploads started should be when it is actually picked for upload
        // https://github.com/opensearch-project/OpenSearch/issues/9729
        remoteTranslogTransferTracker.addUploadBytesStarted(fileTransferTracker.getTotalBytesToUpload());
    }

    /**
     * Adds relevant stats to the tracker when an upload is successfully completed
     */
    private void captureStatsOnUploadSuccess(long prevUploadBytesSucceeded, long prevUploadTimeInMillis) {
        remoteTranslogTransferTracker.setLastSuccessfulUploadTimestamp(System.currentTimeMillis());
        remoteTranslogTransferTracker.incrementTotalUploadsSucceeded();
        long totalUploadedBytes = remoteTranslogTransferTracker.getUploadBytesSucceeded() - prevUploadBytesSucceeded;
        remoteTranslogTransferTracker.updateUploadBytesMovingAverage(totalUploadedBytes);
        long uploadDurationInMillis = remoteTranslogTransferTracker.getTotalUploadTimeInMillis() - prevUploadTimeInMillis;
        remoteTranslogTransferTracker.updateUploadTimeMovingAverage(uploadDurationInMillis);
        if (uploadDurationInMillis > 0) {
            remoteTranslogTransferTracker.updateUploadBytesPerSecMovingAverage((totalUploadedBytes * 1_000L) / uploadDurationInMillis);
        }
    }

    /**
     * Adds relevant stats to the tracker when an upload has failed
     */
    private void captureStatsOnUploadFailure() {
        remoteTranslogTransferTracker.incrementTotalUploadsFailed();
    }

    public boolean downloadTranslog(String primaryTerm, String generation, Path location) throws IOException {
        logger.trace(
            "Downloading translog files with: Primary Term = {}, Generation = {}, Location = {}",
            primaryTerm,
            generation,
            location
        );
        // Download Checkpoint file from remote to local FS
        String ckpFileName = Translog.getCommitCheckpointFileName(Long.parseLong(generation));
        downloadToFS(ckpFileName, location, primaryTerm);
        // Download translog file from remote to local FS
        String translogFilename = Translog.getFilename(Long.parseLong(generation));
        downloadToFS(translogFilename, location, primaryTerm);
        return true;
    }

    private void downloadToFS(String fileName, Path location, String primaryTerm) throws IOException {
        Path filePath = location.resolve(fileName);
        // Here, we always override the existing file if present.
        // We need to change this logic when we introduce incremental download
        if (Files.exists(filePath)) {
            Files.delete(filePath);
        }

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

        // Mark in FileTransferTracker so that the same files are not uploaded at the time of translog sync
        fileTransferTracker.add(fileName, true);
    }

    public TranslogTransferMetadata readMetadata() throws IOException {
        SetOnce<TranslogTransferMetadata> metadataSetOnce = new SetOnce<>();
        SetOnce<IOException> exceptionSetOnce = new SetOnce<>();
        final CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<List<BlobMetadata>> latchedActionListener = new LatchedActionListener<>(
            ActionListener.wrap(blobMetadataList -> {
                if (blobMetadataList.isEmpty()) return;
                RemoteStoreUtils.verifyNoMultipleWriters(
                    blobMetadataList.stream().map(BlobMetadata::name).collect(Collectors.toList()),
                    TranslogTransferMetadata::getNodeIdByPrimaryTermAndGen
                );
                String filename = blobMetadataList.get(0).name();
                boolean downloadStatus = false;
                long downloadStartTime = System.nanoTime(), bytesToRead = 0;
                try (InputStream inputStream = transferService.downloadBlob(remoteMetadataTransferPath, filename)) {
                    // Capture number of bytes for stats before reading
                    bytesToRead = inputStream.available();
                    IndexInput indexInput = new ByteArrayIndexInput("metadata file", inputStream.readAllBytes());
                    metadataSetOnce.set(metadataStreamWrapper.readStream(indexInput));
                    downloadStatus = true;
                } catch (IOException e) {
                    logger.error(() -> new ParameterizedMessage("Exception while reading metadata file: {}", filename), e);
                    exceptionSetOnce.set(e);
                } finally {
                    remoteTranslogTransferTracker.addDownloadTimeInMillis((System.nanoTime() - downloadStartTime) / 1_000_000L);
                    logger.debug("translogMetadataDownloadStatus={}", downloadStatus);
                    if (downloadStatus) {
                        remoteTranslogTransferTracker.addDownloadBytesSucceeded(bytesToRead);
                    }
                }
            }, e -> {
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                }
                logger.error(() -> new ParameterizedMessage("Exception while listing metadata files"), e);
                exceptionSetOnce.set((IOException) e);
            }),
            latch
        );

        try {
            transferService.listAllInSortedOrder(
                remoteMetadataTransferPath,
                TranslogTransferMetadata.METADATA_PREFIX,
                METADATA_FILES_TO_FETCH,
                latchedActionListener
            );
            latch.await();
        } catch (InterruptedException e) {
            throw new IOException("Exception while reading/downloading metadafile", e);
        }

        if (exceptionSetOnce.get() != null) {
            throw exceptionSetOnce.get();
        }

        return metadataSetOnce.get();
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

        return new TransferFileSnapshot(
            translogTransferMetadata.getFileName(),
            getMetadataBytes(translogTransferMetadata),
            translogTransferMetadata.getPrimaryTerm()
        );
    }

    /**
     * Get the metadata bytes for a {@link TranslogTransferMetadata} object
     *
     * @param metadata The object to be parsed
     * @return Byte representation for the given metadata
     */
    public byte[] getMetadataBytes(TranslogTransferMetadata metadata) throws IOException {
        byte[] metadataBytes;

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            try (
                OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput(
                    "translog transfer metadata " + metadata.getPrimaryTerm(),
                    metadata.getFileName(),
                    output,
                    TranslogTransferMetadata.BUFFER_SIZE
                )
            ) {
                metadataStreamWrapper.writeStream(indexOutput, metadata);
            }
            metadataBytes = BytesReference.toBytes(output.bytes());
        }

        return metadataBytes;
    }

    /**
     * This method handles deletion of multiple generations for a single primary term. The deletion happens for translog
     * and metadata files.
     *
     * @param primaryTerm primary term where the generations will be deleted.
     * @param generations set of generation to delete.
     * @param onCompletion runnable to run on completion of deletion regardless of success/failure.
     */
    public void deleteGenerationAsync(long primaryTerm, Set<Long> generations, Runnable onCompletion) {
        List<String> translogFiles = new ArrayList<>();
        generations.forEach(generation -> {
            // Add .ckp and .tlog file to translog file list which is located in basePath/<primaryTerm>
            String ckpFileName = Translog.getCommitCheckpointFileName(generation);
            String translogFileName = Translog.getFilename(generation);
            translogFiles.addAll(List.of(ckpFileName, translogFileName));
        });
        // Delete the translog and checkpoint files asynchronously
        deleteTranslogFilesAsync(primaryTerm, translogFiles, onCompletion);
    }

    /**
     * Deletes all primary terms from remote store that are more than the given {@code minPrimaryTermToKeep}. The caller
     * of the method must ensure that the value is lesser than equal to the minimum primary term referenced by the remote
     * translog metadata.
     *
     * @param minPrimaryTermToKeep all primary terms below this primary term are deleted.
     */
    public void deletePrimaryTermsAsync(long minPrimaryTermToKeep) {
        logger.info("Deleting primary terms from remote store lesser than {}", minPrimaryTermToKeep);
        transferService.listFoldersAsync(ThreadPool.Names.REMOTE_PURGE, remoteDataTransferPath, new ActionListener<>() {
            @Override
            public void onResponse(Set<String> folders) {
                Set<Long> primaryTermsInRemote = folders.stream().filter(folderName -> {
                    try {
                        Long.parseLong(folderName);
                        return true;
                    } catch (Exception ignored) {
                        // NO-OP
                    }
                    return false;
                }).map(Long::parseLong).collect(Collectors.toSet());
                Set<Long> primaryTermsToDelete = primaryTermsInRemote.stream()
                    .filter(term -> term < minPrimaryTermToKeep)
                    .collect(Collectors.toSet());
                primaryTermsToDelete.forEach(term -> deletePrimaryTermAsync(term));
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Exception occurred while getting primary terms from remote store", e);
            }
        });
    }

    /**
     * Handles deletion of all translog files associated with a primary term.
     *
     * @param primaryTerm primary term.
     */
    private void deletePrimaryTermAsync(long primaryTerm) {
        transferService.deleteAsync(
            ThreadPool.Names.REMOTE_PURGE,
            remoteDataTransferPath.add(String.valueOf(primaryTerm)),
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    logger.info("Deleted primary term {}", primaryTerm);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(new ParameterizedMessage("Exception occurred while deleting primary term {}", primaryTerm), e);
                }
            }
        );
    }

    /**
     * Deletes all the translog content related to the underlying shard.
     */
    public void delete() {
        // Delete the translog data content from the remote store.
        delete(remoteDataTransferPath);
        // Delete the translog metadata content from the remote store.
        delete(remoteMetadataTransferPath);
    }

    private void delete(BlobPath path) {
        // cleans up all the translog contents in async fashion for the given path
        transferService.deleteAsync(ThreadPool.Names.REMOTE_PURGE, path, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                logger.info("Deleted all remote translog data at path={}", path);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(new ParameterizedMessage("Exception occurred while cleaning translog at path={}", path), e);
            }
        });
    }

    public void deleteStaleTranslogMetadataFilesAsync(Runnable onCompletion) {
        try {
            transferService.listAllInSortedOrderAsync(
                ThreadPool.Names.REMOTE_PURGE,
                remoteMetadataTransferPath,
                TranslogTransferMetadata.METADATA_PREFIX,
                Integer.MAX_VALUE,
                new ActionListener<>() {
                    @Override
                    public void onResponse(List<BlobMetadata> blobMetadata) {
                        List<String> sortedMetadataFiles = blobMetadata.stream().map(BlobMetadata::name).collect(Collectors.toList());
                        if (sortedMetadataFiles.size() <= 1) {
                            logger.trace("Remote Metadata file count is {}, so skipping deletion", sortedMetadataFiles.size());
                            onCompletion.run();
                            return;
                        }
                        List<String> metadataFilesToDelete = sortedMetadataFiles.subList(1, sortedMetadataFiles.size());
                        logger.trace("Deleting remote translog metadata files {}", metadataFilesToDelete);
                        deleteMetadataFilesAsync(metadataFilesToDelete, onCompletion);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("Exception occurred while listing translog metadata files from remote store", e);
                        onCompletion.run();
                    }
                }
            );
        } catch (Exception e) {
            logger.error("Exception occurred while listing translog metadata files from remote store", e);
            onCompletion.run();
        }
    }

    public void deleteTranslogFiles() throws IOException {
        transferService.delete(remoteMetadataTransferPath);
        transferService.delete(remoteDataTransferPath);
    }

    /**
     * Deletes list of translog files asynchronously using the {@code REMOTE_PURGE} threadpool.
     *
     * @param primaryTerm primary term of translog files.
     * @param files       list of translog files to be deleted.
     * @param onCompletion runnable to run on completion of deletion regardless of success/failure.
     */
    private void deleteTranslogFilesAsync(long primaryTerm, List<String> files, Runnable onCompletion) {
        try {
            transferService.deleteBlobsAsync(
                ThreadPool.Names.REMOTE_PURGE,
                remoteDataTransferPath.add(String.valueOf(primaryTerm)),
                files,
                new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        fileTransferTracker.delete(files);
                        logger.trace("Deleted translogs for primaryTerm={} files={}", primaryTerm, files);
                        onCompletion.run();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onCompletion.run();
                        logger.error(
                            () -> new ParameterizedMessage(
                                "Exception occurred while deleting translog for primaryTerm={} files={}",
                                primaryTerm,
                                files
                            ),
                            e
                        );
                    }
                }
            );
        } catch (Exception e) {
            onCompletion.run();
            throw e;
        }
    }

    /**
     * Deletes metadata files asynchronously using the {@code REMOTE_PURGE} threadpool. On success or failure, runs {@code onCompletion}.
     *
     * @param files list of metadata files to be deleted.
     * @param onCompletion runnable to run on completion of deletion regardless of success/failure.
     */
    private void deleteMetadataFilesAsync(List<String> files, Runnable onCompletion) {
        try {
            transferService.deleteBlobsAsync(ThreadPool.Names.REMOTE_PURGE, remoteMetadataTransferPath, files, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    onCompletion.run();
                    logger.trace("Deleted remote translog metadata files {}", files);
                }

                @Override
                public void onFailure(Exception e) {
                    onCompletion.run();
                    logger.error(new ParameterizedMessage("Exception occurred while deleting remote translog metadata files {}", files), e);
                }
            });
        } catch (Exception e) {
            onCompletion.run();
            throw e;
        }
    }
}

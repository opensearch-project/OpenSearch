/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.logging.Loggers;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.listener.FileTransferListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * FileTransferTracker keeps track of generational translog files uploaded to the remote translog store
 */
public class FileTransferTracker implements FileTransferListener {

    private final Map<String, TransferState> fileTransferTracker;
    private final Map<Long, TransferState> generationTransferTracker;
    private final RemoteTranslogTransferTracker remoteTranslogTransferTracker;
    private Map<String, Long> bytesForTlogCkpFileToUpload;
    private long fileTransferStartTime = -1;
    private final Logger logger;
    private final boolean ckpAsTranslogMetadata;

    public FileTransferTracker(
        ShardId shardId,
        RemoteTranslogTransferTracker remoteTranslogTransferTracker,
        boolean ckpAsTranslogMetadata
    ) {
        this.fileTransferTracker = new ConcurrentHashMap<>();
        this.generationTransferTracker = new ConcurrentHashMap<>();
        this.remoteTranslogTransferTracker = remoteTranslogTransferTracker;
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.ckpAsTranslogMetadata = ckpAsTranslogMetadata;
    }

    void recordFileTransferStartTime(long uploadStartTime) {
        // Recording the start time more than once for a sync is invalid
        if (fileTransferStartTime == -1) {
            fileTransferStartTime = uploadStartTime;
        }
    }

    void recordBytesForFiles(Set<TranslogCheckpointSnapshot> toUpload) {
        bytesForTlogCkpFileToUpload = new HashMap<>();
        toUpload.forEach(file -> {
            recordFileContentLength(file.getTranslogFileName(), file::getTranslogFileContentLength);
            recordFileContentLength(file.getCheckpointFileName(), file::getCheckpointFileContentLength);
        });
    }

    private void recordFileContentLength(String fileName, LongSupplier contentLengthSupplier) {
        if (uploaded(fileName) == false) {
            bytesForTlogCkpFileToUpload.put(fileName, contentLengthSupplier.getAsLong());
        }
    }

    long getTotalBytesToUpload() {
        return bytesForTlogCkpFileToUpload.values().stream().reduce(0L, Long::sum);
    }

    private <K> void updateTransferState(Map<K, TransferState> tracker, K key, TransferState targetState) {
        tracker.compute(key, (k, v) -> {
            if (v == null || v.validateNextState(targetState)) {
                return targetState;
            }
            throw new IllegalStateException("Unexpected transfer state " + v + " while setting target to " + targetState);
        });
    }

    void addGeneration(long generation, boolean success) {
        TransferState targetState = success ? TransferState.SUCCESS : TransferState.FAILED;
        addGeneration(generation, targetState);
    }

    private void addGeneration(long generation, TransferState targetState) {
        updateTransferState(generationTransferTracker, generation, targetState);
    }

    void add(String file, boolean success) {
        TransferState targetState = success ? TransferState.SUCCESS : TransferState.FAILED;
        add(file, targetState);
    }

    private void add(String file, TransferState targetState) {
        updateTransferState(fileTransferTracker, file, targetState);
    }

    @Override
    public void onSuccess(TranslogCheckpointSnapshot fileSnapshot) {
        try {
            long durationInMillis = (System.nanoTime() - fileTransferStartTime) / 1_000_000L;
            remoteTranslogTransferTracker.addUploadTimeInMillis(durationInMillis);
            updateTransferStats(fileSnapshot, true);
        } catch (Exception ex) {
            logger.error("Failure to update translog upload success stats", ex);
        }
        addGeneration(fileSnapshot.getGeneration(), TransferState.SUCCESS);
        if (!ckpAsTranslogMetadata) {
            add(fileSnapshot.getCheckpointFileName(), TransferState.SUCCESS);
            add(fileSnapshot.getTranslogFileName(), TransferState.SUCCESS);
        }
    }

    @Override
    public void onFailure(TranslogCheckpointSnapshot fileSnapshot, Exception e) {
        long durationInMillis = (System.nanoTime() - fileTransferStartTime) / 1_000_000L;
        remoteTranslogTransferTracker.addUploadTimeInMillis(durationInMillis);
        addGeneration(fileSnapshot.getGeneration(), TransferState.FAILED);

        if (ckpAsTranslogMetadata) {
            updateTransferStats(fileSnapshot, false);
        } else {
            assert e instanceof TranslogTransferException;
            TranslogTransferException exception = (TranslogTransferException) e;
            Set<TransferFileSnapshot> failedFiles = exception.getFailedFiles();
            Set<TransferFileSnapshot> successFiles = exception.getSuccessFiles();
            assert failedFiles.isEmpty() == false;
            failedFiles.forEach(failedFile -> {
                add(failedFile.getName(), false);
                long failedBytes = 0;
                try {
                    failedBytes = failedFile.getContentLength();
                } catch (IOException ignore) {}
                updateBytesInRemoteTranslogTransferTracker(failedBytes, false);
            });
            successFiles.forEach(successFile -> {
                add(successFile.getName(), true);
                long succededBytes = 0;
                try {
                    succededBytes = successFile.getContentLength();
                } catch (IOException ignore) {}
                updateBytesInRemoteTranslogTransferTracker(succededBytes, true);
            });
        }
    }

    private void updateTransferStats(TranslogCheckpointSnapshot fileSnapshot, boolean isSuccess) {
        Long translogFileBytes = bytesForTlogCkpFileToUpload.get(fileSnapshot.getTranslogFileName());
        if (translogFileBytes != null) {
            updateBytesInRemoteTranslogTransferTracker(translogFileBytes, isSuccess);
        }

        Long checkpointFileBytes = bytesForTlogCkpFileToUpload.get(fileSnapshot.getCheckpointFileName());
        if (checkpointFileBytes != null) {
            updateBytesInRemoteTranslogTransferTracker(checkpointFileBytes, isSuccess);
        }
    }

    private void updateBytesInRemoteTranslogTransferTracker(long bytes, boolean isSuccess) {
        if (isSuccess) {
            remoteTranslogTransferTracker.addUploadBytesSucceeded(bytes);
        } else {
            remoteTranslogTransferTracker.addUploadBytesFailed(bytes);
        }
    }

    void delete(List<String> names) {
        for (String name : names) {
            fileTransferTracker.remove(name);
        }
    }

    void deleteGenerations(Set<Long> generations) {
        for (Long generation : generations) {
            generationTransferTracker.remove(generation);
        }
    }

    public boolean uploaded(String file) {
        return fileTransferTracker.get(file) == TransferState.SUCCESS;
    }

    boolean translogGenerationUploaded(Long generation) {
        return generationTransferTracker.get(generation) == TransferState.SUCCESS;
    }

    Set<TranslogCheckpointSnapshot> exclusionFilter(Set<TranslogCheckpointSnapshot> original) {
        return original.stream()
            .filter(fileSnapshot -> generationTransferTracker.get(fileSnapshot.getGeneration()) != TransferState.SUCCESS)
            .collect(Collectors.toSet());
    }

    Set<Long> allUploadedGeneration() {
        return getSuccessfulKeys(generationTransferTracker);
    }

    public Set<String> allUploaded() {
        return getSuccessfulKeys(fileTransferTracker);
    }

    private <K> Set<K> getSuccessfulKeys(Map<K, TransferState> tracker) {
        return tracker.entrySet()
            .stream()
            .filter(entry -> entry.getValue() == TransferState.SUCCESS)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    /**
     * Represents the state of the upload operation
     */
    private enum TransferState {
        SUCCESS,
        FAILED;

        public boolean validateNextState(TransferState target) {
            switch (this) {
                case FAILED:
                    return true;
                case SUCCESS:
                    return Objects.equals(SUCCESS, target);
            }
            return false;
        }
    }
}

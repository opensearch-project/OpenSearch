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
import org.opensearch.index.translog.transfer.listener.FileTransferListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * FileTransferTracker keeps track of generational translog files uploaded to the remote translog store
 */
public class FileTransferTracker implements FileTransferListener {

    private final ConcurrentHashMap<String, TransferState> fileTransferTracker;
    private final ConcurrentHashMap<Long, TransferState> generationTransferTracker;
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

    private void recordFileContentLength(String fileName, ThrowingSupplier<Long, IOException> contentLengthSupplier) {
        try {
            if (!uploaded(fileName)) {
                bytesForTlogCkpFileToUpload.put(fileName, contentLengthSupplier.get());
            }
        } catch (IOException ignored) {
            bytesForTlogCkpFileToUpload.put(fileName, 0L);
        }
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T, E extends Exception> {
        T get() throws E;
    }

    long getTotalBytesToUpload() {
        return bytesForTlogCkpFileToUpload.values().stream().reduce(0L, Long::sum);
    }

    private <K> void updateTransferTracker(ConcurrentHashMap<K, TransferState> tracker, K key, TransferState targetState) {
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
        updateTransferTracker(generationTransferTracker, generation, targetState);
    }

    void add(String file, boolean success) {
        TransferState targetState = success ? TransferState.SUCCESS : TransferState.FAILED;
        add(file, targetState);
    }

    private void add(String file, TransferState targetState) {
        updateTransferTracker(fileTransferTracker, file, targetState);
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
        updateTransferStats(fileSnapshot, false);
        addGeneration(fileSnapshot.getGeneration(), TransferState.FAILED);
    }

    private void updateTransferStats(TranslogCheckpointSnapshot fileSnapshot, boolean isSuccess) {
        Long translogFileBytes = bytesForTlogCkpFileToUpload.get(fileSnapshot.getTranslogFileName());
        if (translogFileBytes != null) {
            if (isSuccess) {
                remoteTranslogTransferTracker.addUploadBytesSucceeded(translogFileBytes);
            } else {
                remoteTranslogTransferTracker.addUploadBytesFailed(translogFileBytes);
            }
        }

        Long checkpointFileBytes = bytesForTlogCkpFileToUpload.get(fileSnapshot.getCheckpointFileName());
        if (checkpointFileBytes != null) {
            if (isSuccess) {
                remoteTranslogTransferTracker.addUploadBytesSucceeded(checkpointFileBytes);
            } else {
                remoteTranslogTransferTracker.addUploadBytesFailed(checkpointFileBytes);
            }
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
        Set<Long> successGenFileTransferTracker = new HashSet<>();
        generationTransferTracker.forEach((k, v) -> {
            if (v == TransferState.SUCCESS) {
                successGenFileTransferTracker.add(k);
            }
        });
        return successGenFileTransferTracker;
    }

    public Set<String> allUploaded() {
        Set<String> successFileTransferTracker = new HashSet<>();
        fileTransferTracker.forEach((k, v) -> {
            if (v == TransferState.SUCCESS) {
                successFileTransferTracker.add(k);
            }
        });
        return successFileTransferTracker;
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

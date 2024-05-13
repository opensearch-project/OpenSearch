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

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * FileTransferTracker keeps track of generational translog files uploaded to the remote translog store
 *
 * @opensearch.internal
 */
public abstract class FileTransferTracker {

    final Map<Long, TransferState> generationTransferTracker;
    final RemoteTranslogTransferTracker remoteTranslogTransferTracker;
    Map<String, Long> bytesForTlogCkpFileToUpload;
    long fileTransferStartTime = -1;
    final Logger logger;

    public FileTransferTracker(ShardId shardId, RemoteTranslogTransferTracker remoteTranslogTransferTracker) {
        this.generationTransferTracker = new ConcurrentHashMap<>();
        this.remoteTranslogTransferTracker = remoteTranslogTransferTracker;
        this.logger = Loggers.getLogger(getClass(), shardId);
    }

    void recordFileTransferStartTime(long uploadStartTime) {
        // Recording the start time more than once for a sync is invalid
        if (fileTransferStartTime == -1) {
            fileTransferStartTime = uploadStartTime;
        }
    }

    abstract void recordBytesForFiles(Set<TranslogCheckpointSnapshot> toUpload);

    void recordFileContentLength(String fileName, LongSupplier contentLengthSupplier) {
        bytesForTlogCkpFileToUpload.put(fileName, contentLengthSupplier.getAsLong());
    }

    long getTotalBytesToUpload() {
        return bytesForTlogCkpFileToUpload.values().stream().reduce(0L, Long::sum);
    }

    public abstract void onSuccess(TranslogCheckpointSnapshot fileSnapshot);

    void addGeneration(long generation, boolean success) {
        TransferState targetState = success ? TransferState.SUCCESS : TransferState.FAILED;
        updateTransferState(generationTransferTracker, generation, targetState);
    }

    /**
     * Updates the transfer state for the given key in the specified tracker map.
     */
    <K> void updateTransferState(Map<K, TransferState> tracker, K key, TransferState targetState) {
        tracker.compute(key, (k, v) -> {
            if (v == null || v.validateNextState(targetState)) {
                return targetState;
            }
            throw new IllegalStateException("Unexpected transfer state " + v + " while setting target to " + targetState);
        });
    }

    public abstract void onFailure(TranslogCheckpointSnapshot fileSnapshot, Exception e);

    abstract void deleteGenerations(Set<Long> generations);

    public boolean uploaded(Long generation) {
        return generationTransferTracker.get(generation) == TransferState.SUCCESS;
    }

    Set<TranslogCheckpointSnapshot> exclusionFilter(Set<TranslogCheckpointSnapshot> original) {
        return original.stream()
            .filter(fileSnapshot -> generationTransferTracker.get(fileSnapshot.getGeneration()) != TransferState.SUCCESS)
            .collect(Collectors.toSet());
    }

    public int allUploadedGenerationSize() {
        return generationTransferTracker.size();
    }

    /**
     * @param bytes     bytes to add in remote translog transfer tracker.
     * @param isSuccess represent if provided bytes failed or succeeded
     */
    void updateUploadBytesInRemoteTranslogTransferTracker(long bytes, boolean isSuccess) {
        if (isSuccess) {
            remoteTranslogTransferTracker.addUploadBytesSucceeded(bytes);
        } else {
            remoteTranslogTransferTracker.addUploadBytesFailed(bytes);
        }
    }

    void updateUploadTimeInRemoteTranslogTransferTracker() {
        long durationInMillis = (System.nanoTime() - fileTransferStartTime) / 1_000_000L;
        remoteTranslogTransferTracker.addUploadTimeInMillis(durationInMillis);
    }

    void updateTranslogTransferStats(String fileName, boolean isSuccess) {
        Long fileBytes = bytesForTlogCkpFileToUpload.get(fileName);
        updateUploadBytesInRemoteTranslogTransferTracker(fileBytes, isSuccess);
    }

    /**
     * Represents the state of the upload operation
     */
    enum TransferState {
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

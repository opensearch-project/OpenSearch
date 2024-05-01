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
 * FileTransferTracker keeps track of generational translog files uploaded to the remote translog
 */
public class FileTransferTracker implements FileTransferListener {

    private final ConcurrentHashMap<String, TransferState> fileTransferTracker;
    private final ConcurrentHashMap<Long, TransferState> generationalFilesTransferTracker;
    private final ShardId shardId;
    private final RemoteTranslogTransferTracker remoteTranslogTransferTracker;
    private Map<String, Long> bytesForTlogCkpFileToUpload;
    private long fileTransferStartTime = -1;
    private final Logger logger;
    private final boolean startUploadingTranslogCkpAsMetadata;

    public FileTransferTracker(
        ShardId shardId,
        RemoteTranslogTransferTracker remoteTranslogTransferTracker,
        boolean startTranslogCkpAsObjectMetadata
    ) {
        this.shardId = shardId;
        this.fileTransferTracker = new ConcurrentHashMap<>();
        this.generationalFilesTransferTracker = new ConcurrentHashMap<>();
        this.remoteTranslogTransferTracker = remoteTranslogTransferTracker;
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.startUploadingTranslogCkpAsMetadata = startTranslogCkpAsObjectMetadata;
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
            try {
                bytesForTlogCkpFileToUpload.put(file.getTranslogFileName(), file.getTranslogFileContentLength());
            } catch (IOException ignored) {
                bytesForTlogCkpFileToUpload.put(file.getTranslogFileName(), 0L);
            }
            try {
                bytesForTlogCkpFileToUpload.put(file.getCheckpointFileName(), file.getCheckpointFileContentLength());
            } catch (IOException ignored) {
                bytesForTlogCkpFileToUpload.put(file.getCheckpointFileName(), 0L);
            }
        });
    }

    long getTotalBytesToUpload() {
        return bytesForTlogCkpFileToUpload.values().stream().reduce(0L, Long::sum);
    }

    @Override
    public void onSuccess(TranslogCheckpointSnapshot fileSnapshot) {
        try {
            long durationInMillis = (System.nanoTime() - fileTransferStartTime) / 1_000_000L;
            remoteTranslogTransferTracker.addUploadTimeInMillis(durationInMillis);
            remoteTranslogTransferTracker.addUploadBytesSucceeded(bytesForTlogCkpFileToUpload.get(fileSnapshot.getTranslogFileName()));
            remoteTranslogTransferTracker.addUploadBytesSucceeded(bytesForTlogCkpFileToUpload.get(fileSnapshot.getCheckpointFileName()));
        } catch (Exception ex) {
            logger.error("Failure to update translog upload success stats", ex);
        }
        addGeneration(fileSnapshot.getGeneration(), TransferState.SUCCESS);
        add(fileSnapshot.getTranslogFileName(), TransferState.SUCCESS);
        if (!startUploadingTranslogCkpAsMetadata) {
            add(fileSnapshot.getCheckpointFileName(), TransferState.SUCCESS);
        }
    }

    void addGeneration(long generation, boolean success) {
        TransferState targetState = success ? TransferState.SUCCESS : TransferState.FAILED;
        addGeneration(generation, targetState);
    }

    private void addGeneration(long generation, TransferState targetState) {
        generationalFilesTransferTracker.compute(generation, (k, v) -> {
            if (v == null || v.validateNextState(targetState)) {
                return targetState;
            }
            throw new IllegalStateException("Unexpected transfer state " + v + "while setting target to" + targetState);
        });
    }

    void add(String file, boolean success) {
        TransferState targetState = success ? TransferState.SUCCESS : TransferState.FAILED;
        add(file, targetState);
    }

    private void add(String file, TransferState targetState) {
        fileTransferTracker.compute(file, (k, v) -> {
            if (v == null || v.validateNextState(targetState)) {
                return targetState;
            }
            throw new IllegalStateException("Unexpected transfer state " + v + "while setting target to" + targetState);
        });
    }

    @Override
    public void onFailure(TranslogCheckpointSnapshot fileSnapshot, Exception e) {
        long durationInMillis = (System.nanoTime() - fileTransferStartTime) / 1_000_000L;
        remoteTranslogTransferTracker.addUploadTimeInMillis(durationInMillis);
        remoteTranslogTransferTracker.addUploadBytesFailed(bytesForTlogCkpFileToUpload.get(fileSnapshot.getTranslogFileName()));
        remoteTranslogTransferTracker.addUploadBytesFailed(bytesForTlogCkpFileToUpload.get(fileSnapshot.getCheckpointFileName()));
        addGeneration(fileSnapshot.getGeneration(), TransferState.FAILED);
    }

    public void delete(List<String> names) {
        for (String name : names) {
            fileTransferTracker.remove(name);
        }
    }

    public void deleteGenerations(Set<Long> generations) {
        for (Long generation : generations) {
            generationalFilesTransferTracker.remove(generation);
        }
    }

    public boolean uploaded(String file) {
        return fileTransferTracker.get(file) == TransferState.SUCCESS;
    }

    public boolean translogGenerationUploaded(Long generation) {
        return generationalFilesTransferTracker.get(generation) == TransferState.SUCCESS;
    }

    public Set<TranslogCheckpointSnapshot> exclusionFilter(Set<TranslogCheckpointSnapshot> original) {
        return original.stream()
            .filter(fileSnapshot -> generationalFilesTransferTracker.get(fileSnapshot.getGeneration()) != TransferState.SUCCESS)
            .collect(Collectors.toSet());
    }

    public Set<Long> allUploadedGeneration() {
        Set<Long> successGenFileTransferTracker = new HashSet<>();
        generationalFilesTransferTracker.forEach((k, v) -> {
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

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * FileTransferTracker keeps track of translog files uploaded to the remote translog
 */
public class FileTransferTracker implements FileTransferListener {

    private final ConcurrentHashMap<String, TransferState> fileTransferTracker;
    private final ShardId shardId;
    private final RemoteTranslogTransferTracker remoteTranslogTransferTracker;
    private Map<String, Long> bytesForTlogCkpFileToUpload;
    private long fileTransferStartTime = -1;
    private final Logger logger;

    public FileTransferTracker(ShardId shardId, RemoteTranslogTransferTracker remoteTranslogTransferTracker) {
        this.shardId = shardId;
        this.fileTransferTracker = new ConcurrentHashMap<>();
        this.remoteTranslogTransferTracker = remoteTranslogTransferTracker;
        this.logger = Loggers.getLogger(getClass(), shardId);
    }

    void recordFileTransferStartTime(long uploadStartTime) {
        // Recording the start time more than once for a sync is invalid
        if (fileTransferStartTime == -1) {
            fileTransferStartTime = uploadStartTime;
        }
    }

    void recordBytesForFiles(Set<TransferFileSnapshot> toUpload) {
        bytesForTlogCkpFileToUpload = new HashMap<>();
        toUpload.forEach(file -> {
            try {
                bytesForTlogCkpFileToUpload.put(file.getName(), file.getContentLength());
            } catch (IOException ignored) {
                bytesForTlogCkpFileToUpload.put(file.getName(), 0L);
            }
        });
    }

    long getTotalBytesToUpload() {
        return bytesForTlogCkpFileToUpload.values().stream().reduce(0L, Long::sum);
    }

    @Override
    public void onSuccess(TransferFileSnapshot fileSnapshot) {
        try {
            long durationInMillis = (System.nanoTime() - fileTransferStartTime) / 1_000_000L;
            remoteTranslogTransferTracker.addUploadTimeInMillis(durationInMillis);
            remoteTranslogTransferTracker.addUploadBytesSucceeded(bytesForTlogCkpFileToUpload.get(fileSnapshot.getName()));
        } catch (Exception ex) {
            logger.error("Failure to update translog upload success stats", ex);
        }

        add(fileSnapshot.getName(), TransferState.SUCCESS);
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
    public void onFailure(TransferFileSnapshot fileSnapshot, Exception e) {
        long durationInMillis = (System.nanoTime() - fileTransferStartTime) / 1_000_000L;
        remoteTranslogTransferTracker.addUploadTimeInMillis(durationInMillis);
        remoteTranslogTransferTracker.addUploadBytesFailed(bytesForTlogCkpFileToUpload.get(fileSnapshot.getName()));
        add(fileSnapshot.getName(), TransferState.FAILED);
    }

    public void delete(List<String> names) {
        for (String name : names) {
            fileTransferTracker.remove(name);
        }
    }

    public boolean uploaded(String file) {
        return fileTransferTracker.get(file) == TransferState.SUCCESS;
    }

    public Set<TransferFileSnapshot> exclusionFilter(Set<TransferFileSnapshot> original) {
        return original.stream()
            .filter(fileSnapshot -> fileTransferTracker.get(fileSnapshot.getName()) != TransferState.SUCCESS)
            .collect(Collectors.toSet());
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

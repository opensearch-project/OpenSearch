/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.index.shard.ShardId;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;
import org.opensearch.index.translog.transfer.listener.FileTransferListener;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * FileTransferTracker keeps track of translog files uploaded to the remote translog
 */
public class FileTransferTracker implements FileTransferListener {

    private final ConcurrentHashMap<String, TransferState> fileTransferTracker;
    private final ShardId shardId;

    public FileTransferTracker(ShardId shardId) {
        this.shardId = shardId;
        this.fileTransferTracker = new ConcurrentHashMap<>();
    }

    @Override
    public void onSuccess(TransferFileSnapshot fileSnapshot) {
        TransferState targetState = TransferState.SUCCESS;
        fileTransferTracker.compute(fileSnapshot.getName(), (k, v) -> {
            if (v == null || v.validateNextState(targetState)) {
                return targetState;
            }
            throw new IllegalStateException("Unexpected transfer state " + v + "while setting target to" + targetState);
        });
    }

    @Override
    public void onFailure(TransferFileSnapshot fileSnapshot, Exception e) {
        TransferState targetState = TransferState.FAILED;
        fileTransferTracker.compute(fileSnapshot.getName(), (k, v) -> {
            if (v == null || v.validateNextState(targetState)) {
                return targetState;
            }
            throw new IllegalStateException("Unexpected transfer state " + v + "while setting target to" + targetState);
        });
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
                    return Set.of(SUCCESS).contains(target);
            }
            return false;
        }
    }
}

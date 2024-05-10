/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A subclass of {@link FileTransferTracker} that tracks the transfer state of translog files for generation
 * when translog ckp and translog tlog file are uploaded separately
 *
 * @opensearch.internal
 */
public class TranslogCkpFilesTransferTracker extends FileTransferTracker {

    private final Map<String, TransferState> fileTransferTracker;

    public TranslogCkpFilesTransferTracker(ShardId shardId, RemoteTranslogTransferTracker remoteTranslogTransferTracker) {
        super(shardId, remoteTranslogTransferTracker);
        fileTransferTracker = new ConcurrentHashMap<>();
    }

    public void onSuccess(TranslogCheckpointSnapshot fileSnapshot) {
        try {
            updateUploadTimeInRemoteTranslogTransferTracker();
            String tlogFileName = fileSnapshot.getTranslogFileName();
            String ckpFileName = fileSnapshot.getCheckpointFileName();
            if (isUploaded(tlogFileName) == false) {
                updateTranslogTransferStats(tlogFileName, true);
            }
            if (isUploaded(ckpFileName) == false) {
                updateTranslogTransferStats(ckpFileName, true);
            }
        } catch (Exception ex) {
            logger.error("Failure to update translog upload success stats", ex);
        }
        addGeneration(fileSnapshot.getGeneration(), true);
        addFile(fileSnapshot.getCheckpointFileName(), true);
        addFile(fileSnapshot.getTranslogFileName(), true);
    }

    public void onFailure(TranslogCheckpointSnapshot fileSnapshot, Exception e) {
        updateUploadTimeInRemoteTranslogTransferTracker();
        addGeneration(fileSnapshot.getGeneration(), false);

        assert e instanceof TranslogTransferException;
        TranslogTransferException exception = (TranslogTransferException) e;
        Set<TransferFileSnapshot> failedFiles = exception.getFailedFiles();
        Set<TransferFileSnapshot> successFiles = exception.getSuccessFiles();
        assert failedFiles.isEmpty() == false;
        failedFiles.forEach(failedFile -> {
            addFile(failedFile.getName(), false);
            updateTranslogTransferStats(failedFile.getName(), false);
        });
        successFiles.forEach(successFile -> {
            addFile(successFile.getName(), true);
            updateTranslogTransferStats(successFile.getName(), true);
        });
    }

    public void addFile(String file, boolean success) {
        TransferState targetState = success ? TransferState.SUCCESS : TransferState.FAILED;
        updateTransferState(fileTransferTracker, file, targetState);
    }

    @Override
    public boolean isUploaded(String file) {
        return fileTransferTracker.get(file) == TransferState.SUCCESS;
    }

    @Override
    public Set<String> allUploaded() {
        return getSuccessfulKeys(fileTransferTracker);
    }

    // here along with generation we also mark status of files in the tracker.
    @Override
    void addGeneration(long generation, boolean success) {
        TransferState targetState = success ? TransferState.SUCCESS : TransferState.FAILED;
        updateTransferState(generationTransferTracker, Long.toString(generation), targetState);

        // add files as well.
        String tlogFileName = Translog.getFilename(generation);
        String ckpFileName = Translog.getCommitCheckpointFileName(generation);
        addFile(tlogFileName, success);
        addFile(ckpFileName, success);
    }

    @Override
    void recordBytesForFiles(Set<TranslogCheckpointSnapshot> toUpload) {
        bytesForTlogCkpFileToUpload = new HashMap<>();
        toUpload.forEach(file -> {
            String tlogFileName = file.getTranslogFileName();
            String ckpFileName = file.getCheckpointFileName();
            if (isUploaded(tlogFileName) == false) {
                recordFileContentLength(tlogFileName, file::getTranslogFileContentLength);
            }
            if (isUploaded(ckpFileName) == false) {
                recordFileContentLength(ckpFileName, file::getCheckpointFileContentLength);
            }
        });
    }

    @Override
    void deleteGenerations(Set<Long> generations) {
        for (Long generation : generations) {
            String tlogFileName = Translog.getFilename(generation);
            String ckpFileName = Translog.getCommitCheckpointFileName(generation);
            generationTransferTracker.remove(Long.toString(generation));
            fileTransferTracker.remove(tlogFileName);
            fileTransferTracker.remove(ckpFileName);
        }
    }

}

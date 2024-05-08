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
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;

import java.util.Set;

/**
 * A subclass of {@link FileTransferTracker} that tracks the transfer state of translog files for generation
 * when translog ckp and translog tlog file are uploaded separately
 *
 * @opensearch.internal
 */
public class TranslogCkpFilesTransferTracker extends FileTransferTracker {

    public TranslogCkpFilesTransferTracker(ShardId shardId, RemoteTranslogTransferTracker remoteTranslogTransferTracker) {
        super(shardId, remoteTranslogTransferTracker);
    }

    public void onSuccess(TranslogCheckpointSnapshot fileSnapshot) {
        try {
            updateUploadTimeInRemoteTranslogTransferTracker();
            String tlogFileName = fileSnapshot.getTranslogFileName();
            String ckpFileName = fileSnapshot.getCheckpointFileName();
            if (isFileUploaded(tlogFileName) == false) {
                updateTranslogTransferStats(tlogFileName, true);
            }
            if (isFileUploaded(ckpFileName) == false) {
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

}

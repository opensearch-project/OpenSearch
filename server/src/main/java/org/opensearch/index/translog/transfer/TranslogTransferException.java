/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;

import java.util.HashSet;
import java.util.Set;

/**
 * Exception when a generation of translog files transfer encounters a failure
 *
 * @opensearch.internal
 */
public class TranslogTransferException extends RuntimeException {

    private final TranslogCheckpointSnapshot fileSnapshot;
    private final Set<TransferFileSnapshot> failedFiles;
    private final Set<TransferFileSnapshot> successFiles;

    public TranslogTransferException(
        TranslogCheckpointSnapshot fileSnapshot,
        Throwable cause,
        Set<TransferFileSnapshot> failedFiles,
        Set<TransferFileSnapshot> successFiles
    ) {
        super(cause);
        this.fileSnapshot = fileSnapshot;
        this.failedFiles = failedFiles == null ? new HashSet<>() : failedFiles;
        this.successFiles = successFiles == null ? new HashSet<>() : successFiles;
    }

    public TranslogCheckpointSnapshot getFileSnapshot() {
        return fileSnapshot;
    }

    public Set<TransferFileSnapshot> getFailedFiles() {
        return failedFiles;
    }

    public Set<TransferFileSnapshot> getSuccessFiles() {
        return successFiles;
    }
}

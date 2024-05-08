/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.Nullable;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;

import java.util.Collections;
import java.util.Set;

import reactor.util.annotation.NonNull;

/**
 * Exception when a generation of translog files transfer encounters a failure
 *
 * @opensearch.internal
 */
public class TranslogTransferException extends RuntimeException {

    private final TranslogCheckpointSnapshot fileSnapshot;
    private final Set<TransferFileSnapshot> failedFiles;
    private final Set<TransferFileSnapshot> successfulFiles;

    public TranslogTransferException(
        TranslogCheckpointSnapshot fileSnapshot,
        Throwable cause,
        @NonNull Set<TransferFileSnapshot> failedFiles,
        @Nullable Set<TransferFileSnapshot> successfulFiles
    ) {
        super(cause);
        this.fileSnapshot = fileSnapshot;
        this.successfulFiles = successfulFiles == null ? Collections.emptySet() : successfulFiles;
        this.failedFiles = failedFiles;
    }

    public TranslogCheckpointSnapshot getFileSnapshot() {
        return fileSnapshot;
    }

    public Set<TransferFileSnapshot> getFailedFiles() {
        return failedFiles;
    }

    public Set<TransferFileSnapshot> getSuccessFiles() {
        return successfulFiles;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

/**
 * Exception when a generation of translog files transfer encounters a failure
 *
 * @opensearch.internal
 */
public class TranslogGenerationTransferException extends RuntimeException {

    private final TranslogCheckpointSnapshot fileSnapshot;

    public TranslogGenerationTransferException(TranslogCheckpointSnapshot fileSnapshot, Throwable cause) {
        super(cause);
        this.fileSnapshot = fileSnapshot;
    }

    public TranslogCheckpointSnapshot getFileSnapshot() {
        return fileSnapshot;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;

public class FileTransferException extends RuntimeException {

    private final TransferFileSnapshot fileSnapshot;

    public FileTransferException(TransferFileSnapshot fileSnapshot, Throwable cause) {
        super(cause);
        this.fileSnapshot = fileSnapshot;
    }

    public FileTransferException(TransferFileSnapshot fileSnapshot, String message, Throwable cause) {
        super(message, cause);
        this.fileSnapshot = fileSnapshot;
    }

    public TransferFileSnapshot getFileSnapshot() {
        return fileSnapshot;
    }
}

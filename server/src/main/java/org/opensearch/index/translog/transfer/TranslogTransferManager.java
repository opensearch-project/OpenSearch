/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.index.translog.TranslogReader;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class TranslogTransferManager {

    private final TransferService transferService;
    private final TranslogTransferListener translogTransferListener;

    public TranslogTransferManager(TransferService transferService, TranslogTransferListener translogTransferListener) {
        this.transferService = transferService;
        this.translogTransferListener = translogTransferListener;
    }

    boolean uploadTranslog(Path path, List<TranslogReader> translogReader) throws IOException {
        TranslogUploadSnapshot translogUploadSnapshot = new TranslogUploadSnapshot(path, translogReader);
        return false;
    }
}

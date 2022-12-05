/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.action.ActionListener;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;

import java.io.IOException;

/**
 * Interface for the translog transfer service responsible for interacting with a remote store
 *
 * @opensearch.internal
 */
public interface TransferService {

    /**
     * Uploads the {@link TransferFileSnapshot} async, once the upload is complete the callback is invoked
     * @param fileSnapshot the file snapshot to upload
     * @param remotePath the remote path where upload should be made
     * @param listener the callback to be invoked once upload completes successfully/fails
     */
    void uploadBlobAsync(
        final TransferFileSnapshot fileSnapshot,
        Iterable<String> remotePath,
        ActionListener<TransferFileSnapshot> listener
    );

    /**
     * Uploads the {@link TransferFileSnapshot} blob
     * @param fileSnapshot the file snapshot to upload
     * @param remotePath the remote path where upload should be made
     * @throws IOException the exception while transferring the data
     */
    void uploadBlob(final TransferFileSnapshot fileSnapshot, Iterable<String> remotePath) throws IOException;

}

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
import java.io.InputStream;
import java.util.Set;

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

    /**
     * Lists the files
     * @param path : the path to list
     * @return : the lists of files
     * @throws IOException the exception while listing the path
     */
    Set<String> listAll(Iterable<String> path) throws IOException;

    /**
     *
     * @param path
     * @param fileName
     * @return inputstream of the remote file
     * @throws IOException the exception while reading the data
     */
    InputStream downloadBlob(Iterable<String> path, String fileName) throws IOException;

}

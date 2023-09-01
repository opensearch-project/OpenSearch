/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.blobstore.stream.read.listener.ReadContextListener;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;

/**
 * An extension of {@link BlobContainer} that adds {@link AsyncMultiStreamBlobContainer#asyncBlobUpload} to allow
 * multipart uploads and performs integrity checks on transferred files
 *
 * @opensearch.internal
 */
public interface AsyncMultiStreamBlobContainer extends BlobContainer {

    /**
     * Reads blob content from multiple streams, each from a specific part of the file, which is provided by the
     * StreamContextSupplier in the WriteContext passed to this method. An {@link IOException} is thrown if reading
     * any of the input streams fails, or writing to the target blob fails
     *
     * @param writeContext         A WriteContext object encapsulating all information needed to perform the upload
     * @param completionListener   Listener on which upload events should be published.
     * @throws IOException if any of the input streams could not be read, or the target blob could not be written to
     */
    void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException;

    /**
     * Creates an async callback of a {@link ReadContext} containing the multipart streams for a specified blob within the container.
     * @param blobName The name of the blob for which the {@link ReadContext} needs to be fetched.
     * @param listener  Async listener for {@link ReadContext} object which serves the input streams and other metadata for the blob
     */
    @ExperimentalApi
    void readBlobAsync(String blobName, ActionListener<ReadContext> listener);

    /**
     * Asynchronously downloads the blob to the specified location using an executor from the thread pool.
     * @param blobName The name of the blob for which needs to be downloaded.
     * @param fileLocation The path on local disk where the blob needs to be downloaded.
     * @param threadPool The threadpool instance which will provide the executor for performing a multipart download.
     * @param completionListener Listener which will be notified when the download is complete.
     */
    @ExperimentalApi
    default void asyncBlobDownload(String blobName, Path fileLocation, ThreadPool threadPool, ActionListener<String> completionListener) {
        ReadContextListener readContextListener = new ReadContextListener(blobName, fileLocation, threadPool, completionListener);
        readBlobAsync(blobName, readContextListener);
    }

    /*
     * Wether underlying blobContainer can verify integrity of data after transfer. If true and if expected
     * checksum is provided in WriteContext, then the checksum of transferred data is compared with expected checksum
     * by underlying blobContainer. In this case, caller doesn't need to ensure integrity of data.
     */
    boolean remoteIntegrityCheckSupported();
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.blobstore.stream.write.WriteContext;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * An extension of {@link BlobContainer} that adds {@link VerifyingMultiStreamBlobContainer#writeBlobByStreams} to allow
 * multipart uploads
 *
 * @opensearch.internal
 */
public interface VerifyingMultiStreamBlobContainer extends BlobContainer {

    /**
     * Reads blob content from multiple streams, each from a specific part of the file, which is provided by the
     * StreamContextSupplier in the WriteContext passed to this method. An {@link IOException} is thrown if reading
     * any of the input streams fails, or writing to the target blob fails
     *
     * @param writeContext A WriteContext object encapsulating all information needed to perform the upload
     * @return A {@link CompletableFuture} representing the upload
     * @throws IOException if any of the input streams could not be read, or the target blob could not be written to
     */
    CompletableFuture<Void> writeBlobByStreams(WriteContext writeContext) throws IOException;
}

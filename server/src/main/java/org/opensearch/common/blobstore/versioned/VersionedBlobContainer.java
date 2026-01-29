/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.versioned;

import org.opensearch.common.Nullable;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.support.AbstractBlobContainer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Abstract blob container that supports conditional operations using version tokens.
 * Provides version-based read and write operations for optimistic concurrency control.
 *
 * @opensearch.internal
 */
public abstract class VersionedBlobContainer extends AbstractBlobContainer {

    protected VersionedBlobContainer(BlobPath path) {
        super(path);
    }


    /**
     * Writes a blob with conditional version support.
     *
     * @param blobName the name of the blob
     * @param inputStream the input stream to write
     * @param blobSize the size of the blob
     * @param expectedVersion the expected version for conditional write
     * @return VersionedInputStream containing the new version
     * @throws IOException if write fails or version mismatch
     */
    public abstract String conditionallyWriteBlobWithVersion(
        String blobName,
        InputStream inputStream,
        long blobSize,
        String expectedVersion
    ) throws IOException;

    /**
     * Writes a blob if it does not exists and get the version
     *
     * @param blobName the name of the blob
     * @param inputStream the input stream to write
     * @param blobSize the size of the blob
     * @return VersionedInputStream containing the new version
     * @throws IOException if write fails or version mismatch
     */
    public abstract String writeVersionedBlobIfNotExists(
        String blobName,
        InputStream inputStream,
        long blobSize
    ) throws IOException;

    /**
     * Reads a versioned blob
     *
     * @param blobName the name of the blob
     * @return VersionedInputStream containing the input stream and version
     * @throws IOException if read fails
     */
    public abstract VersionedInputStream readVersionedBlob(String blobName) throws IOException;

    /**
     * Gets the current version of a blob without reading its content.
     *
     * @param blobName the name of the blob
     * @return the current version
     * @throws IOException if blob doesn't exist or operation fails
     */
    public abstract String getVersion(String blobName) throws IOException;
}

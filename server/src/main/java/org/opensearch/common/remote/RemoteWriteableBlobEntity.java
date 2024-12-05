/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.remote;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.compress.Compressor;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.PATH_DELIMITER;

/**
 * The abstract class which represents a {@link RemoteWriteableEntity} that can be written to a store
 * @param <T> the entity to be written
 */
public abstract class RemoteWriteableBlobEntity<T> implements RemoteWriteableEntity<T> {

    protected String blobFileName;

    protected String blobName;
    private final String clusterUUID;
    private final Compressor compressor;
    private String[] pathTokens;

    public RemoteWriteableBlobEntity(final String clusterUUID, final Compressor compressor) {
        this.clusterUUID = clusterUUID;
        this.compressor = compressor;
    }

    public abstract BlobPathParameters getBlobPathParameters();

    public abstract String getType();

    public String getFullBlobName() {
        return blobName;
    }

    public String getBlobFileName() {
        if (blobFileName == null) {
            String[] pathTokens = getBlobPathTokens();
            if (pathTokens == null || pathTokens.length < 1) {
                return null;
            }
            blobFileName = pathTokens[pathTokens.length - 1];
        }
        return blobFileName;
    }

    public String[] getBlobPathTokens() {
        if (pathTokens != null) {
            return pathTokens;
        }
        if (blobName == null) {
            return null;
        }
        pathTokens = blobName.split(PATH_DELIMITER);
        return pathTokens;
    }

    public abstract String generateBlobFileName();

    /**
     * Generate the blob path for the remote entity by adding a custom prefix.
     * This custom prefix may be generated by any of the strategies defined in {@link org.opensearch.index.remote.RemoteStoreEnums}
     * The default implementation returns the same path as passed in the argument.
     * @param blobPath The remote path on which the remote entity is to be uploaded
     * @return The modified remote path after adding a custom prefix at which the remote entity will be uploaded.
     */
    public BlobPath getPrefixedPath(BlobPath blobPath) {
        return blobPath;
    }

    public String clusterUUID() {
        return clusterUUID;
    }

    public void setFullBlobName(BlobPath blobPath) {
        this.blobName = blobPath.buildAsString() + blobFileName;
    }

    protected Compressor getCompressor() {
        return compressor;
    }

}

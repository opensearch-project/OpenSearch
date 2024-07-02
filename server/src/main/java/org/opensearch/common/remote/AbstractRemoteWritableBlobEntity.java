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
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.PATH_DELIMITER;

/**
 * An extension of {@link RemoteWriteableEntity} class which caters to the use case of writing to and reading from a blob storage
 *
 * @param <T> The class type which can be uploaded to or downloaded from a blob storage.
 */
public abstract class AbstractRemoteWritableBlobEntity<T> implements RemoteWriteableEntity<T> {

    protected String blobFileName;

    protected String blobName;
    private final String clusterUUID;
    private final Compressor compressor;
    private final NamedXContentRegistry namedXContentRegistry;
    private String[] pathTokens;

    public AbstractRemoteWritableBlobEntity(
        final String clusterUUID,
        final Compressor compressor,
        final NamedXContentRegistry namedXContentRegistry
    ) {
        this.clusterUUID = clusterUUID;
        this.compressor = compressor;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    public AbstractRemoteWritableBlobEntity(final String clusterUUID, final Compressor compressor) {
        this(clusterUUID, compressor, null);
    }

    public AbstractRemoteWritableBlobEntity() {
        this(null, null, null);
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

    public String clusterUUID() {
        return clusterUUID;
    }

    public abstract UploadedMetadata getUploadedMetadata();

    public void setFullBlobName(BlobPath blobPath) {
        this.blobName = blobPath.buildAsString() + blobFileName;
    }

    public NamedXContentRegistry getNamedXContentRegistry() {
        return namedXContentRegistry;
    }

    protected Compressor getCompressor() {
        return compressor;
    }

}

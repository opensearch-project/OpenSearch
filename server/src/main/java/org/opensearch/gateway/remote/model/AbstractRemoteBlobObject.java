/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.compress.Compressor;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.PATH_DELIMITER;

/**
 * An extension of {@link RemoteObject} class which caters to the use case of writing to and reading from a blob storage
 *
 * @param <T> The class type which can be uploaded to or downloaded from a blob storage.
 */
public abstract class AbstractRemoteBlobObject<T> implements RemoteObject<T> {

    protected String blobFileName;

    protected String blobName;
    private final BlobStoreRepository blobStoreRepository;
    private final String clusterUUID;

    public AbstractRemoteBlobObject(BlobStoreRepository blobStoreRepository, String clusterUUID) {
        this.blobStoreRepository = blobStoreRepository;
        this.clusterUUID = clusterUUID;
    }

    public abstract BlobPathParameters getBlobPathParameters();

    public String getFullBlobName() {
        return blobName;
    }

    public String getBlobFileName() {
        if (blobFileName == null) {
            String[] pathTokens = blobName.split(PATH_DELIMITER);
            blobFileName = pathTokens[pathTokens.length - 1];
        }
        return blobFileName;
    }

    public abstract String generateBlobFileName();

    public String clusterUUID() {
        return clusterUUID;
    }

    public abstract UploadedMetadata getUploadedMetadata();

    protected void setFullBlobName(BlobPath blobPath) {
        this.blobName = blobPath.buildAsString() + blobFileName;
    }

    protected Compressor getCompressor() {
        return blobStoreRepository.getCompressor();
    }

    protected BlobStoreRepository getBlobStoreRepository() {
        return this.blobStoreRepository;
    }

}

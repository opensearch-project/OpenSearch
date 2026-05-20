/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.remote;

import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;

/**
 * An extension of {@link RemoteWriteableEntity} class which caters to the use case of writing to and reading from a blob storage
 *
 * @param <T> The class type which can be uploaded to or downloaded from a blob storage.
 */
public abstract class AbstractClusterMetadataWriteableBlobEntity<T> extends RemoteWriteableBlobEntity<T> {

    protected final NamedXContentRegistry namedXContentRegistry;

    public AbstractClusterMetadataWriteableBlobEntity(
        final String clusterUUID,
        final Compressor compressor,
        final NamedXContentRegistry namedXContentRegistry
    ) {
        super(clusterUUID, compressor);
        this.namedXContentRegistry = namedXContentRegistry;
    }

    public AbstractClusterMetadataWriteableBlobEntity(final String clusterUUID, final Compressor compressor) {
        super(clusterUUID, compressor);
        this.namedXContentRegistry = null;
    }

    public abstract UploadedMetadata getUploadedMetadata();

    public NamedXContentRegistry getNamedXContentRegistry() {
        return namedXContentRegistry;
    }
}

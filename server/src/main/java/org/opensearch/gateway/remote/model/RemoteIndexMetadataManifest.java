/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.common.io.Streams;
import org.opensearch.common.remote.AbstractClusterMetadataWriteableBlobEntity;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.gateway.remote.IndexMetadataManifest;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;

/**
 * Wrapper class for uploading/downloading {@link IndexMetadataManifest} to/from remote blob store
 */
public class RemoteIndexMetadataManifest extends AbstractClusterMetadataWriteableBlobEntity<IndexMetadataManifest> {

    public static final String INDEX_METADATA_MANIFEST = "index-metadata-manifest";
    public static final String INDEX_METADATA_MANIFEST_NAME_FORMAT = "%s";

    public static final ChecksumBlobStoreFormat<IndexMetadataManifest> INDEX_METADATA_MANIFEST_FORMAT = 
        new ChecksumBlobStoreFormat<>(
            "index-metadata-manifest",
            INDEX_METADATA_MANIFEST_NAME_FORMAT,
            IndexMetadataManifest::fromXContent
        );

    private IndexMetadataManifest indexMetadataManifest;

    public RemoteIndexMetadataManifest(
        final IndexMetadataManifest indexMetadataManifest,
        final String clusterUUID,
        final Compressor compressor,
        final NamedXContentRegistry namedXContentRegistry
    ) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.indexMetadataManifest = indexMetadataManifest;
        setClusterUUIDAgnostic(true);
    }

    public RemoteIndexMetadataManifest(
        final String blobName,
        final String clusterUUID,
        final Compressor compressor,
        final NamedXContentRegistry namedXContentRegistry
    ) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.blobName = blobName;
        setClusterUUIDAgnostic(true);
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of(INDEX_METADATA_MANIFEST), INDEX_METADATA_MANIFEST);
    }

    @Override
    public String getType() {
        return INDEX_METADATA_MANIFEST;
    }

    @Override
    public String generateBlobFileName() {
        // index-metadata-manifest__{codec_version}
        String blobFileName = String.join(
            DELIMITER,
            INDEX_METADATA_MANIFEST,
            String.valueOf(indexMetadataManifest.getCodecVersion())
        );
        this.blobFileName = blobFileName;
        return blobFileName;
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new UploadedMetadataAttribute(INDEX_METADATA_MANIFEST, blobName);
    }

    @Override
    public InputStream serialize() throws IOException {
        return INDEX_METADATA_MANIFEST_FORMAT.serialize(
            indexMetadataManifest,
            generateBlobFileName(),
            getCompressor(),
            RemoteClusterStateUtils.FORMAT_PARAMS
        ).streamInput();
    }

    @Override
    public IndexMetadataManifest deserialize(final InputStream inputStream) throws IOException {
        return INDEX_METADATA_MANIFEST_FORMAT.deserialize(
            blobName,
            getNamedXContentRegistry(),
            Streams.readFully(inputStream)
        );
    }
}
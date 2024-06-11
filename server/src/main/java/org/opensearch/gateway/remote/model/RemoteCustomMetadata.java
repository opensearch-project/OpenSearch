/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.Metadata.Custom;
import org.opensearch.common.io.Streams;
import org.opensearch.common.remote.AbstractRemoteWritableBlobEntity;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_PATH_TOKEN;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_NAME_PLAIN_FORMAT;

/**
 * Wrapper class for uploading/downloading {@link Custom} to/from remote blob store
 */
public class RemoteCustomMetadata extends AbstractRemoteWritableBlobEntity<Custom> {

    public static final String CUSTOM_METADATA = "custom";
    public static final String CUSTOM_DELIMITER = "--";
    public final ChecksumBlobStoreFormat<Custom> customBlobStoreFormat;

    private Custom custom;
    private final String customType;
    private long metadataVersion;

    public RemoteCustomMetadata(
        final Custom custom,
        final String customType,
        final long metadataVersion,
        final String clusterUUID,
        Compressor compressor,
        NamedXContentRegistry namedXContentRegistry
    ) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.custom = custom;
        this.customType = customType;
        this.metadataVersion = metadataVersion;
        this.customBlobStoreFormat = new ChecksumBlobStoreFormat<>(
            "custom",
            METADATA_NAME_PLAIN_FORMAT,
            (parser -> Metadata.Custom.fromXContent(parser, customType))
        );
    }

    public RemoteCustomMetadata(
        final String blobName,
        final String customType,
        final String clusterUUID,
        final Compressor compressor,
        final NamedXContentRegistry namedXContentRegistry
    ) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.blobName = blobName;
        this.customType = customType;
        this.customBlobStoreFormat = new ChecksumBlobStoreFormat<>(
            "custom",
            METADATA_NAME_PLAIN_FORMAT,
            (parser -> Metadata.Custom.fromXContent(parser, customType))
        );
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        String prefix = String.join(CUSTOM_DELIMITER, CUSTOM_METADATA, customType);
        return new BlobPathParameters(List.of(GLOBAL_METADATA_PATH_TOKEN), prefix);
    }

    @Override
    public String generateBlobFileName() {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/global-metadata/<componentPrefix>__<inverted_metadata_version>__<inverted__timestamp>__
        // <codec_version>
        String blobFileName = String.join(
            DELIMITER,
            getBlobPathParameters().getFilePrefix(),
            RemoteStoreUtils.invertLong(metadataVersion),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(GLOBAL_METADATA_CURRENT_CODEC_VERSION)
        );
        this.blobFileName = blobFileName;
        return blobFileName;
    }

    @Override
    public InputStream serialize() throws IOException {
        return customBlobStoreFormat.serialize(custom, generateBlobFileName(), getCompressor(), RemoteClusterStateUtils.FORMAT_PARAMS)
            .streamInput();
    }

    @Override
    public Custom deserialize(final InputStream inputStream) throws IOException {
        return customBlobStoreFormat.deserialize(blobName, getNamedXContentRegistry(), Streams.readFully(inputStream));
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new UploadedMetadataAttribute(String.join(CUSTOM_DELIMITER, CUSTOM_METADATA, customType), blobName);
    }
}

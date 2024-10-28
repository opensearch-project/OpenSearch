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
import org.opensearch.common.settings.Settings;
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
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_NAME_PLAIN_FORMAT;

/**
 * Wrapper class for uploading/downloading persistent {@link Settings} to/from remote blob store
 */
public class RemotePersistentSettingsMetadata extends AbstractClusterMetadataWriteableBlobEntity<Settings> {

    public static final String SETTING_METADATA = "settings";

    public static final ChecksumBlobStoreFormat<Settings> SETTINGS_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "settings",
        METADATA_NAME_PLAIN_FORMAT,
        Settings::fromXContent
    );

    private Settings persistentSettings;
    private long metadataVersion;

    public RemotePersistentSettingsMetadata(
        final Settings settings,
        final long metadataVersion,
        final String clusterUUID,
        final Compressor compressor,
        final NamedXContentRegistry namedXContentRegistry
    ) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.persistentSettings = settings;
        this.metadataVersion = metadataVersion;
    }

    public RemotePersistentSettingsMetadata(
        final String blobName,
        final String clusterUUID,
        final Compressor compressor,
        final NamedXContentRegistry namedXContentRegistry
    ) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.blobName = blobName;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of("global-metadata"), SETTING_METADATA);
    }

    @Override
    public String getType() {
        return SETTING_METADATA;
    }

    @Override
    public String generateBlobFileName() {
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
        return SETTINGS_METADATA_FORMAT.serialize(
            persistentSettings,
            generateBlobFileName(),
            getCompressor(),
            RemoteClusterStateUtils.FORMAT_PARAMS
        ).streamInput();
    }

    @Override
    public Settings deserialize(final InputStream inputStream) throws IOException {
        return SETTINGS_METADATA_FORMAT.deserialize(blobName, getNamedXContentRegistry(), Streams.readFully(inputStream));
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new UploadedMetadataAttribute(SETTING_METADATA, blobName);
    }
}

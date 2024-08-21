/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.cluster.metadata.DiffableStringMap;
import org.opensearch.common.io.Streams;
import org.opensearch.common.remote.AbstractClusterMetadataWriteableBlobEntity;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.core.compress.Compressor;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.ChecksumWritableBlobStoreFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_PATH_TOKEN;

/**
 * Wrapper class for uploading/downloading {@link DiffableStringMap} to/from remote blob store
 */
public class RemoteHashesOfConsistentSettings extends AbstractClusterMetadataWriteableBlobEntity<DiffableStringMap> {
    public static final String HASHES_OF_CONSISTENT_SETTINGS = "hashes-of-consistent-settings";
    public static final ChecksumWritableBlobStoreFormat<DiffableStringMap> HASHES_OF_CONSISTENT_SETTINGS_FORMAT =
        new ChecksumWritableBlobStoreFormat<>("hashes-of-consistent-settings", DiffableStringMap::readFrom);

    private DiffableStringMap hashesOfConsistentSettings;
    private long metadataVersion;

    public RemoteHashesOfConsistentSettings(
        final DiffableStringMap hashesOfConsistentSettings,
        final long metadataVersion,
        final String clusterUUID,
        final Compressor compressor
    ) {
        super(clusterUUID, compressor, null);
        this.metadataVersion = metadataVersion;
        this.hashesOfConsistentSettings = hashesOfConsistentSettings;
    }

    public RemoteHashesOfConsistentSettings(final String blobName, final String clusterUUID, final Compressor compressor) {
        super(clusterUUID, compressor, null);
        this.blobName = blobName;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of(GLOBAL_METADATA_PATH_TOKEN), HASHES_OF_CONSISTENT_SETTINGS);
    }

    @Override
    public String getType() {
        return HASHES_OF_CONSISTENT_SETTINGS;
    }

    @Override
    public String generateBlobFileName() {
        String blobFileName = String.join(
            DELIMITER,
            getBlobPathParameters().getFilePrefix(),
            RemoteStoreUtils.invertLong(metadataVersion),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION)
        );
        this.blobFileName = blobFileName;
        return blobFileName;
    }

    @Override
    public ClusterMetadataManifest.UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new ClusterMetadataManifest.UploadedMetadataAttribute(HASHES_OF_CONSISTENT_SETTINGS, blobName);
    }

    @Override
    public InputStream serialize() throws IOException {
        return HASHES_OF_CONSISTENT_SETTINGS_FORMAT.serialize(
            (out, settingsHash) -> settingsHash.writeTo(out),
            hashesOfConsistentSettings,
            generateBlobFileName(),
            getCompressor()
        ).streamInput();
    }

    @Override
    public DiffableStringMap deserialize(final InputStream inputStream) throws IOException {
        return HASHES_OF_CONSISTENT_SETTINGS_FORMAT.deserialize(blobName, Streams.readFully(inputStream));
    }
}

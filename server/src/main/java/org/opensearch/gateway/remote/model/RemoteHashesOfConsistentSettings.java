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
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.remote.AbstractRemoteWritableBlobEntity;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.compress.Compressor;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.index.remote.RemoteStoreUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.opensearch.core.common.bytes.BytesReference.toBytes;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_PATH_TOKEN;

/**
 * Wrapper class for uploading/downloading {@link DiffableStringMap} to/from remote blob store
 */
public class RemoteHashesOfConsistentSettings extends AbstractRemoteWritableBlobEntity<DiffableStringMap> {
    public static final String HASHES_OF_CONSISTENT_SETTINGS = "hashes-of-consistent-settings";

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
        try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
            hashesOfConsistentSettings.writeTo(bytesStreamOutput);
            return bytesStreamOutput.bytes().streamInput();
        } catch (IOException e) {
            throw new IOException("Failed to serialize hashes of consistent settings", e);
        }

    }

    @Override
    public DiffableStringMap deserialize(final InputStream inputStream) throws IOException {
        try (StreamInput in = new BytesStreamInput(toBytes(Streams.readFully(inputStream)))) {
            return DiffableStringMap.readFrom(in);
        } catch (IOException e) {
            throw new IOException("Failed to deserialize hashes of consistent settings", e);
        }
    }
}

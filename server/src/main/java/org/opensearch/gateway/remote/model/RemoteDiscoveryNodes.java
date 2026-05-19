/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.io.Streams;
import org.opensearch.common.remote.AbstractClusterMetadataWriteableBlobEntity;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.core.compress.Compressor;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.ChecksumWritableBlobStoreFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.CLUSTER_STATE_EPHEMERAL_PATH_TOKEN;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;

/**
 * Wrapper class for uploading/downloading {@link DiscoveryNodes} to/from remote blob store
 */
public class RemoteDiscoveryNodes extends AbstractClusterMetadataWriteableBlobEntity<DiscoveryNodes> {

    public static final String DISCOVERY_NODES = "nodes";
    public final ChecksumWritableBlobStoreFormat<DiscoveryNodes> discoveryNodesFormat;

    private DiscoveryNodes discoveryNodes;
    private long stateVersion;

    public RemoteDiscoveryNodes(
        final DiscoveryNodes discoveryNodes,
        final long stateVersion,
        final String clusterUUID,
        final Compressor compressor
    ) {
        super(clusterUUID, compressor, null);
        this.discoveryNodes = discoveryNodes;
        this.stateVersion = stateVersion;
        this.discoveryNodesFormat = new ChecksumWritableBlobStoreFormat<>("nodes", is -> DiscoveryNodes.readFrom(is, null));
    }

    public RemoteDiscoveryNodes(final String blobName, final String clusterUUID, final Compressor compressor, final Version version) {
        super(clusterUUID, compressor, null);
        this.blobName = blobName;
        this.discoveryNodesFormat = new ChecksumWritableBlobStoreFormat<>("nodes", is -> DiscoveryNodes.readFrom(is, null), version);
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of(CLUSTER_STATE_EPHEMERAL_PATH_TOKEN), DISCOVERY_NODES);
    }

    @Override
    public String getType() {
        return DISCOVERY_NODES;
    }

    @Override
    public String generateBlobFileName() {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/ephemeral/<componentPrefix>__<inverted_state_version>__<inverted__timestamp>__<codec_version>
        String blobFileName = String.join(
            DELIMITER,
            getBlobPathParameters().getFilePrefix(),
            RemoteStoreUtils.invertLong(stateVersion),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION)
        );
        this.blobFileName = blobFileName;
        return blobFileName;
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new UploadedMetadataAttribute(DISCOVERY_NODES, blobName);
    }

    @Override
    public InputStream serialize() throws IOException {
        return discoveryNodesFormat.serialize(
            (out, discoveryNode) -> discoveryNode.writeToWithAttribute(out),
            discoveryNodes,
            generateBlobFileName(),
            getCompressor()
        ).streamInput();
    }

    @Override
    public DiscoveryNodes deserialize(final InputStream inputStream) throws IOException {
        return discoveryNodesFormat.deserialize(blobName, Streams.readFully(inputStream));
    }
}

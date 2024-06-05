/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import java.io.IOException;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterState.Custom;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.gateway.remote.RemoteClusterStateUtils.RemoteStateTransferException;
import org.opensearch.common.remote.AbstractRemoteWritableBlobEntity;
import org.opensearch.gateway.remote.model.RemoteClusterStateBlobStore;
import org.opensearch.gateway.remote.model.RemoteClusterBlocks;
import org.opensearch.gateway.remote.model.RemoteClusterStateCustoms;
import org.opensearch.gateway.remote.model.RemoteDiscoveryNodes;
import org.opensearch.gateway.remote.model.RemoteReadResult;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.opensearch.gateway.remote.model.RemoteClusterStateCustoms.CLUSTER_STATE_CUSTOM;
import static org.opensearch.gateway.remote.model.RemoteCustomMetadata.CUSTOM_DELIMITER;

public class RemoteClusterStateAttributesManager {
    public static final String CLUSTER_STATE_ATTRIBUTE = "cluster_state_attribute";
    public static final String DISCOVERY_NODES = "nodes";
    public static final String CLUSTER_BLOCKS = "blocks";
    public static final int CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION = 1;
    private final RemoteClusterStateBlobStore<ClusterBlocks, RemoteClusterBlocks> clusterBlocksBlobStore;
    private final RemoteClusterStateBlobStore<DiscoveryNodes, RemoteDiscoveryNodes> discoveryNodesBlobStore;
    private final RemoteClusterStateBlobStore<Custom, RemoteClusterStateCustoms> customsBlobStore;
    private final Compressor compressor;
    private final NamedXContentRegistry namedXContentRegistry;

    RemoteClusterStateAttributesManager(
        RemoteClusterStateBlobStore<ClusterBlocks, RemoteClusterBlocks> clusterBlocksBlobStore, RemoteClusterStateBlobStore<DiscoveryNodes, RemoteDiscoveryNodes> discoveryNodesBlobStore, RemoteClusterStateBlobStore<Custom, RemoteClusterStateCustoms> customsBlobStore, Compressor compressor, NamedXContentRegistry namedXContentRegistry) {
        this.clusterBlocksBlobStore = clusterBlocksBlobStore;
        this.discoveryNodesBlobStore = discoveryNodesBlobStore;
        this.customsBlobStore = customsBlobStore;
        this.compressor = compressor;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    /**
     * Allows async upload of Cluster State Attribute components to remote
     */
    CheckedRunnable<IOException> getAsyncMetadataWriteAction(
        ClusterState clusterState,
        String component,
        ToXContent componentData,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    ) {
        if (componentData instanceof DiscoveryNodes) {
            RemoteDiscoveryNodes remoteObject = new RemoteDiscoveryNodes((DiscoveryNodes)componentData, clusterState.version(), clusterState.metadata().clusterUUID(), compressor, namedXContentRegistry);
            return () -> discoveryNodesBlobStore.writeAsync(remoteObject, getActionListener(component, remoteObject, latchedActionListener));
        } else if (componentData instanceof ClusterBlocks) {
            RemoteClusterBlocks remoteObject = new RemoteClusterBlocks((ClusterBlocks) componentData, clusterState.version(), clusterState.metadata().clusterUUID(), compressor, namedXContentRegistry);
            return () -> clusterBlocksBlobStore.writeAsync(remoteObject, getActionListener(component, remoteObject, latchedActionListener));
        } else if (componentData instanceof ClusterState.Custom) {
            RemoteClusterStateCustoms remoteObject = new RemoteClusterStateCustoms(
                (ClusterState.Custom) componentData,
                component,
                clusterState.version(),
                clusterState.metadata().clusterUUID(),
                compressor,
                namedXContentRegistry
            );
            return () -> customsBlobStore.writeAsync(remoteObject, getActionListener(component, remoteObject, latchedActionListener));
        } else {
            throw new RemoteStateTransferException("Remote object not found for "+ componentData.getClass());
        }
    }

    private ActionListener<Void> getActionListener(String component, AbstractRemoteWritableBlobEntity remoteObject, LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener) {
        return ActionListener.wrap(
            resp -> latchedActionListener.onResponse(
                remoteObject.getUploadedMetadata()
            ),
            ex -> latchedActionListener.onFailure(new RemoteClusterStateUtils.RemoteStateTransferException(component, ex))
        );
    }

    public CheckedRunnable<IOException> getAsyncMetadataReadAction(
        String clusterUUID,
        String component,
        String componentName,
        String uploadedFilename,
        LatchedActionListener<RemoteReadResult> listener
    ) {
        final ActionListener actionListener = ActionListener.wrap(response -> listener.onResponse(new RemoteReadResult((ToXContent) response, CLUSTER_STATE_ATTRIBUTE, component)), listener::onFailure);
        if (component.equals(RemoteDiscoveryNodes.DISCOVERY_NODES)) {
            RemoteDiscoveryNodes remoteDiscoveryNodes = new RemoteDiscoveryNodes(uploadedFilename, clusterUUID, compressor, namedXContentRegistry);
            return () -> discoveryNodesBlobStore.readAsync(remoteDiscoveryNodes, actionListener);
        } else if (component.equals(RemoteClusterBlocks.CLUSTER_BLOCKS)) {
            RemoteClusterBlocks remoteClusterBlocks = new RemoteClusterBlocks(uploadedFilename, clusterUUID, compressor, namedXContentRegistry);
            return () -> clusterBlocksBlobStore.readAsync(remoteClusterBlocks, actionListener);
        } else if (component.equals(CLUSTER_STATE_CUSTOM)) {
            final ActionListener customActionListener = ActionListener.wrap(response -> listener.onResponse(new RemoteReadResult((ToXContent) response, CLUSTER_STATE_ATTRIBUTE, String.join(CUSTOM_DELIMITER, component, componentName))), listener::onFailure);
            RemoteClusterStateCustoms remoteClusterStateCustoms = new RemoteClusterStateCustoms(uploadedFilename, componentName, clusterUUID, compressor, namedXContentRegistry);
            return () -> customsBlobStore.readAsync(remoteClusterStateCustoms, customActionListener);
        } else {
            throw new RemoteStateTransferException("Remote object not found for "+ component);
        }
    }

    public Map<String, ClusterState.Custom> getUpdatedCustoms(ClusterState clusterState, ClusterState previousClusterState) {
        Map<String, ClusterState.Custom> updatedCustoms = new HashMap<>();
        Set<String> currentCustoms = new HashSet<>(clusterState.customs().keySet());
        for (Map.Entry<String, ClusterState.Custom> entry : previousClusterState.customs().entrySet()) {
            if (currentCustoms.contains(entry.getKey()) && !entry.getValue().equals(clusterState.customs().get(entry.getKey()))) {
                updatedCustoms.put(entry.getKey(), clusterState.customs().get(entry.getKey()));
            }
            currentCustoms.remove(entry.getKey());
        }
        for (String custom : currentCustoms) {
            updatedCustoms.put(custom, clusterState.customs().get(custom));
        }
        return updatedCustoms;
    }
}

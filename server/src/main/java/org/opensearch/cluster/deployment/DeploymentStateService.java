/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.deployment;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Read-only service for querying deployment state. Safe to use on any node.
 *
 * @opensearch.internal
 */
public class DeploymentStateService {

    private final Supplier<ClusterState> clusterStateSupplier;

    public DeploymentStateService(Supplier<ClusterState> clusterStateSupplier) {
        this.clusterStateSupplier = clusterStateSupplier;
    }

    public Deployment getDeployment(String deploymentId) {
        DeploymentMetadata metadata = clusterStateSupplier.get().metadata().custom(DeploymentMetadata.TYPE);
        if (metadata == null) {
            return null;
        }
        return metadata.getDeployments().get(deploymentId);
    }

    public Map<String, Deployment> getAllDeployments() {
        DeploymentMetadata metadata = clusterStateSupplier.get().metadata().custom(DeploymentMetadata.TYPE);
        if (metadata == null) {
            return Collections.emptyMap();
        }
        return metadata.getDeployments();
    }

    /**
     * Returns deployment states for all nodes that are part of any active deployment.
     * Used by {@link org.opensearch.cluster.routing.OperationRouting} to filter search traffic.
     */
    public static Map<String, DeploymentState> getNodeDeploymentStates(ClusterState clusterState) {
        DeploymentMetadata metadata = clusterState.metadata().custom(DeploymentMetadata.TYPE);
        if (metadata == null || metadata.getDeployments().isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, DeploymentState> nodeStates = new HashMap<>();
        for (Deployment deployment : metadata.getDeployments().values()) {
            for (DiscoveryNode node : clusterState.nodes()) {
                if (nodeMatchesAttributes(node, deployment.getNodeAttributes())) {
                    nodeStates.put(node.getId(), deployment.getState());
                }
            }
        }
        return nodeStates;
    }

    /**
     * Returns the deployment state for a specific node, or null if the node is not part of any deployment.
     * Used by {@link DeploymentAllocationDecider}.
     */
    public static DeploymentState getNodeDeploymentState(Metadata metadata, DiscoveryNode node) {
        DeploymentMetadata deploymentMetadata = metadata.custom(DeploymentMetadata.TYPE);
        if (deploymentMetadata == null) {
            return null;
        }
        for (Deployment deployment : deploymentMetadata.getDeployments().values()) {
            if (nodeMatchesAttributes(node, deployment.getNodeAttributes())) {
                return deployment.getState();
            }
        }
        return null;
    }

    private static boolean nodeMatchesAttributes(DiscoveryNode node, Map<String, String> targetAttributes) {
        for (Map.Entry<String, String> entry : targetAttributes.entrySet()) {
            String nodeValue = node.getAttributes().get(entry.getKey());
            if (!entry.getValue().equals(nodeValue)) {
                return false;
            }
        }
        return true;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.deployment;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.AckedRequest;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.service.ClusterManagerTask;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.core.action.ActionListener;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Service that manages deployment lifecycle by writing DeploymentMetadata to cluster state.
 * Runs on the elected cluster manager node only.
 *
 * @opensearch.internal
 */
public class DeploymentManagerService {

    private final ClusterService clusterService;
    private final RerouteService rerouteService;
    private final ClusterManagerTaskThrottler.ThrottlingKey startDeploymentTaskKey;
    private final ClusterManagerTaskThrottler.ThrottlingKey finishDeploymentTaskKey;

    public DeploymentManagerService(ClusterService clusterService, RerouteService rerouteService) {
        this.clusterService = clusterService;
        this.rerouteService = rerouteService;
        this.startDeploymentTaskKey = clusterService.registerClusterManagerTask(ClusterManagerTask.START_DEPLOYMENT, true);
        this.finishDeploymentTaskKey = clusterService.registerClusterManagerTask(ClusterManagerTask.FINISH_DEPLOYMENT, true);
    }

    public void startDeployment(
        String deploymentId,
        Map<String, String> nodeAttributes,
        AckedRequest request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        try {
            validateDeploymentId(deploymentId);
            validateAttributes(nodeAttributes);
        } catch (IllegalArgumentException e) {
            listener.onFailure(e);
            return;
        }

        clusterService.submitStateUpdateTask("start-deployment-" + deploymentId, new AckedClusterStateUpdateTask<>(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return innerStartDeployment(deploymentId, nodeAttributes, currentState);
            }

            @Override
            public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                return startDeploymentTaskKey;
            }

            @Override
            protected AcknowledgedResponse newResponse(boolean acknowledged) {
                return new AcknowledgedResponse(acknowledged);
            }

            @Override
            public void onAllNodesAcked(Exception e) {
                super.onAllNodesAcked(e);
                rerouteService.reroute("deployment drain started", Priority.HIGH, ActionListener.wrap(() -> {}));
            }
        });
    }

    public void finishDeployment(String deploymentId, AckedRequest request, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("finish-deployment-" + deploymentId, new AckedClusterStateUpdateTask<>(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return innerFinishDeployment(deploymentId, currentState);
            }

            @Override
            public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                return finishDeploymentTaskKey;
            }

            @Override
            protected AcknowledgedResponse newResponse(boolean acknowledged) {
                return new AcknowledgedResponse(acknowledged);
            }

            @Override
            public void onAllNodesAcked(Exception e) {
                super.onAllNodesAcked(e);
            }
        });
    }

    public static ClusterState innerStartDeployment(String deploymentId, Map<String, String> nodeAttributes, ClusterState currentState) {
        DeploymentMetadata currentMetadata = currentState.metadata().custom(DeploymentMetadata.TYPE);
        Map<String, Deployment> deployments;
        if (currentMetadata != null) {
            deployments = new HashMap<>(currentMetadata.getDeployments());
            validateConsistentKeys(deployments, nodeAttributes);
            validateNoOverlappingValues(deployments, deploymentId, nodeAttributes);
        } else {
            deployments = new HashMap<>();
        }

        deployments.put(deploymentId, new Deployment(DeploymentState.DRAIN, nodeAttributes));

        return ClusterState.builder(currentState)
            .metadata(
                Metadata.builder(currentState.getMetadata()).putCustom(DeploymentMetadata.TYPE, new DeploymentMetadata(deployments)).build()
            )
            .build();
    }

    public static ClusterState innerFinishDeployment(String deploymentId, ClusterState currentState) {
        DeploymentMetadata currentMetadata = currentState.metadata().custom(DeploymentMetadata.TYPE);
        if (currentMetadata == null || !currentMetadata.getDeployments().containsKey(deploymentId)) {
            // Stopping a non-existent deployment should be a no-op
            return currentState;
        }

        Map<String, Deployment> deployments = new HashMap<>(currentMetadata.getDeployments());
        deployments.remove(deploymentId);

        Metadata.Builder metadataBuilder = Metadata.builder(currentState.getMetadata());
        if (deployments.isEmpty()) {
            metadataBuilder.removeCustom(DeploymentMetadata.TYPE);
        } else {
            metadataBuilder.putCustom(DeploymentMetadata.TYPE, new DeploymentMetadata(deployments));
        }

        return ClusterState.builder(currentState).metadata(metadataBuilder.build()).build();
    }

    private static void validateDeploymentId(String deploymentId) {
        if (deploymentId == null || deploymentId.isEmpty()) {
            throw new IllegalArgumentException("deployment ID must not be empty");
        }
        if (deploymentId.startsWith("_")) {
            throw new IllegalArgumentException("deployment ID must not start with '_'");
        }
        for (int i = 0; i < deploymentId.length(); i++) {
            if (Character.isWhitespace(deploymentId.charAt(i))) {
                throw new IllegalArgumentException("deployment ID must not contain whitespace");
            }
        }
    }

    private static void validateAttributes(Map<String, String> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            throw new IllegalArgumentException("attributes must not be empty");
        }
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            String value = entry.getValue();
            if (value.contains("*")) {
                throw new IllegalArgumentException("attribute value [" + value + "] must not contain '*'");
            }
        }
    }

    private static void validateConsistentKeys(Map<String, Deployment> existingDeployments, Map<String, String> newAttributes) {
        if (existingDeployments.isEmpty()) {
            return;
        }
        for (Deployment existingDeployment : existingDeployments.values()) {
            Set<String> existingKeys = existingDeployment.getNodeAttributes().keySet();
            if (!existingKeys.equals(newAttributes.keySet())) {
                throw new IllegalArgumentException(
                    "deployment attribute keys " + newAttributes.keySet() + " must match existing deployment keys " + existingKeys
                );
            }
        }
    }

    private static void validateNoOverlappingValues(
        Map<String, Deployment> existingDeployments,
        String newDeploymentId,
        Map<String, String> newAttributes
    ) {
        for (Map.Entry<String, Deployment> entry : existingDeployments.entrySet()) {
            if (entry.getKey().equals(newDeploymentId)) {
                // We allow idempotent application of start deployment.
                Deployment existingDeploymentWithSameId = entry.getValue();
                if (existingDeploymentWithSameId.getState() != DeploymentState.DRAIN) {
                    throw new IllegalArgumentException(
                        "deployment [" + entry.getKey() + "] is already in state " + existingDeploymentWithSameId.getState()
                    );
                }
                if (existingDeploymentWithSameId.getNodeAttributes().equals(newAttributes) == false) {
                    throw new IllegalArgumentException(
                        "deployment [" + entry.getKey() + "] already targets attributes " + existingDeploymentWithSameId.getNodeAttributes()
                    );
                }
            } else {
                for (Map.Entry<String, String> nodeAttr : entry.getValue().getNodeAttributes().entrySet()) {
                    if (nodeAttr.getValue().equals(newAttributes.get(nodeAttr.getKey()))) {
                        throw new IllegalArgumentException(
                            "deployment [" + entry.getKey() + "] already targets attribute " + nodeAttr.getKey() + "=" + nodeAttr.getValue()
                        );
                    }
                }
            }
        }
    }
}

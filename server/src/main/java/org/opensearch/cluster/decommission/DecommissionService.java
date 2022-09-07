/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.metadata.DecommissionAttributeMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;

/**
 * Service responsible for entire lifecycle of decommissioning and recommissioning an awareness attribute.
 * <p>
 * Whenever a cluster manager initiates operation to decommission an awareness attribute,
 * the service makes the best attempt to perform the following task -
 * <ul>
 * <li>Remove cluster-manager eligible nodes from voting config [TODO - checks to avoid quorum loss scenarios]</li>
 * <li>Initiates nodes decommissioning by adding custom metadata with the attribute and state as {@link DecommissionStatus#DECOMMISSION_INIT}</li>
 * <li>Triggers weigh away for nodes having given awareness attribute to drain. This marks the decommission status as {@link DecommissionStatus#DECOMMISSION_IN_PROGRESS}</li>
 * <li>Once weighed away, the service triggers nodes decommission</li>
 * <li>Once the decommission is successful, the service clears the voting config and marks the status as {@link DecommissionStatus#DECOMMISSION_SUCCESSFUL}</li>
 * <li>If service fails at any step, it would mark the status as {@link DecommissionStatus#DECOMMISSION_FAILED}</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class DecommissionService {

    private static final Logger logger = LogManager.getLogger(DecommissionService.class);

    private final ClusterService clusterService;

    @Inject
    public DecommissionService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void clearDecommissionStatus(final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(
                "delete_decommission_state",
                new ClusterStateUpdateTask(Priority.URGENT) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return deleteDecommissionAttribute(currentState);
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error(() -> new ParameterizedMessage("Failed to clear decommission attribute."), e);
                        listener.onFailure(e);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        // Once the cluster state is processed we can try to recommission nodes by setting the weights for the zone.
                        // TODO Set the weights for the recommissioning zone.
                        listener.onResponse(new ClusterStateUpdateResponse(true));
                    }
                }
        );
    }

    ClusterState deleteDecommissionAttribute(final ClusterState currentState) {
        logger.info("Delete decommission request received");
        Metadata metadata = currentState.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(metadata);
        mdBuilder.removeCustom(DecommissionAttributeMetadata.TYPE);
        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }
}

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
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsResponse;
import org.opensearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsRequest;
import org.opensearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.metadata.DecommissionAttributeMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

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
    private volatile List<String> awarenessAttributes;

    @Inject
    public DecommissionService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void registerRecommissionAttribute(
            final DecommissionAttribute recommissionAttribute,
            final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(
                "delete_decommission [" + recommissionAttribute + "]",
                new ClusterStateUpdateTask(Priority.URGENT) {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return addRecommissionAttributeToCluster(currentState, recommissionAttribute);
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error(() -> new ParameterizedMessage(
                                "failed to mark status as DECOMMISSION_FAILED for decommission attribute [{}]",
                                recommissionAttribute.toString()), e);
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

    ClusterState addRecommissionAttributeToCluster(final ClusterState currentState, final DecommissionAttribute recommissionAttribute) {
        logger.info("Delete decommission request for attribute [{}] received", recommissionAttribute.toString());
        Metadata metadata = currentState.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(metadata);
        DecommissionAttributeMetadata decommissionAttributeMetadata = metadata.custom(DecommissionAttributeMetadata.TYPE);

        if (!isValidRecommission(decommissionAttributeMetadata, recommissionAttribute)) {
            throw new DecommissionFailedException(recommissionAttribute, "Recommission only allowed for decommissioned zone");
        }

        decommissionAttributeMetadata = new DecommissionAttributeMetadata(recommissionAttribute, DecommissionStatus.RECOMMISSION_IN_PROGRESS);
        mdBuilder.putCustom(DecommissionAttributeMetadata.TYPE, decommissionAttributeMetadata);
        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }

    public boolean isValidRecommission(DecommissionAttributeMetadata decommissionAttributeMetadata, DecommissionAttribute recommissionAttribute) {
        if (decommissionAttributeMetadata != null
                && (decommissionAttributeMetadata.status().equals(DecommissionStatus.DECOMMISSION_SUCCESSFUL)
                || decommissionAttributeMetadata.status().equals(DecommissionStatus.DECOMMISSION_IN_PROGRESS))
                && decommissionAttributeMetadata.decommissionAttribute().attributeValue().equals(recommissionAttribute.attributeValue())) {
            return true;
        }
        return false;
    }
}
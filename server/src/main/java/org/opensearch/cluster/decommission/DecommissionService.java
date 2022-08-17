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
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.coordination.NodeRemovalClusterStateTaskExecutor;
import org.opensearch.cluster.metadata.DecommissionAttributeMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

// do we need to implement ClusterStateApplier -> will a change in cluster state impact this service??
public class DecommissionService implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(DecommissionService.class);

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;
    private final ThreadPool threadPool;
    private final DecommissionHelper decommissionHelper;
    private ClusterState clusterState;
    private volatile List<String> awarenessAttributes;

    @Inject
    public DecommissionService(
        Settings settings,
        ClusterSettings clusterSettings,
        ClusterService clusterService,
        TransportService transportService,
        ThreadPool threadPool,
        AllocationService allocationService
    ) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.threadPool = threadPool;
        this.clusterState = clusterService.state(); // TODO - check if this is the right way
        this.nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, logger);
        this.decommissionHelper = new DecommissionHelper(
            clusterService,
            nodeRemovalExecutor
        );
        this.awarenessAttributes = AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
            this::setAwarenessAttributes
        );
    }

    List<String> getAwarenessAttributes() {
        return awarenessAttributes;
    }

    private void setAwarenessAttributes(List<String> awarenessAttributes) {
        this.awarenessAttributes = awarenessAttributes;
    }

    public void initiateAttributeDecommissioning(
        final DecommissionAttribute decommissionAttribute,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        /**
         * 1. Abdicate master
         * 2. Register attribute -> status should be set to INIT
         * 3. Trigger weigh away for graceful decommission -> status should be set to DECOMMISSIONING
         * 4. Once zone is weighed away -> trigger zone decommission using executor -> status should be set to DECOMMISSIONED on successful response
         * 5. Clear voting config
         */
        registerDecommissionAttribute(decommissionAttribute, listener);
    }

    /**
     * Registers new decommissioned attribute metadata in the cluster state
     * <p>
     * This method can be only called on the cluster-manager node. It tries to create a new decommissioned attribute on the master
     * and if it was successful it adds new decommissioned attribute to cluster metadata.
     * <p>
     * This method should only be called once the eligible cluster manager node having decommissioned attribute is abdicated
     *
     * @param decommissionAttribute register decommission attribute in the metadata request
     * @param listener              register decommission listener
     */
    private void registerDecommissionAttribute(
        final DecommissionAttribute decommissionAttribute,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        validateAwarenessAttribute(decommissionAttribute, getAwarenessAttributes());
        clusterService.submitStateUpdateTask(
            "put_decommission [" + decommissionAttribute + "]",
            new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    logger.info("decommission request for attribute [{}] received", decommissionAttribute.toString());
                    Metadata metadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(metadata);
                    DecommissionAttributeMetadata decommissionAttributeMetadata = metadata.custom(DecommissionAttributeMetadata.TYPE);
                    ensureNoAwarenessAttributeDecommissioned(decommissionAttributeMetadata, decommissionAttribute);
                    decommissionAttributeMetadata = new DecommissionAttributeMetadata(decommissionAttribute);
                    mdBuilder.putCustom(DecommissionAttributeMetadata.TYPE, decommissionAttributeMetadata);
                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    // TODO - should we put the weights back to zone, since we weighed away the zone before we started registering the metadata
                    // TODO - should we modify logic of logging for ease of debugging?
                    if (e instanceof DecommissionFailedException) {
                        logger.error(() -> new ParameterizedMessage("failed to decommission attribute [{}]", decommissionAttribute.toString()), e);
                    } else {
                        clusterService.submitStateUpdateTask(
                            "decommission_failed",
                            new ClusterStateUpdateTask(Priority.URGENT) {
                                @Override
                                public ClusterState execute(ClusterState currentState) throws Exception {
                                    Metadata metadata = currentState.metadata();
                                    Metadata.Builder mdBuilder = Metadata.builder(metadata);
                                    logger.info("decommission request for attribute [{}] failed", decommissionAttribute.toString());
                                    DecommissionAttributeMetadata decommissionAttributeMetadata = new DecommissionAttributeMetadata(
                                        decommissionAttribute,
                                        DecommissionStatus.DECOMMISSION_FAILED
                                    );
                                    mdBuilder.putCustom(DecommissionAttributeMetadata.TYPE, decommissionAttributeMetadata);
                                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                                }

                                @Override
                                public void onFailure(String source, Exception e) {
                                    logger.error(() -> new ParameterizedMessage(
                                        "failed to mark status as DECOMMISSION_FAILED for decommission attribute [{}]",
                                        decommissionAttribute.toString()), e);
//                                    listener.onFailure(e);
                                }
                            }
                        );
                    }
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    if (!newState.equals(oldState)) {
                        // TODO - drain the nodes before decommissioning
                        failDecommissionedNodes(newState);
                        listener.onResponse(new ClusterStateUpdateResponse(true));
                    } else {
                        listener.onResponse(new ClusterStateUpdateResponse(false));
                    }
                }
            }
        );
    }

    private static void validateAwarenessAttribute(final DecommissionAttribute decommissionAttribute, List<String> awarenessAttributes) {
        if (!awarenessAttributes.contains(decommissionAttribute.attributeName())) {
            throw new DecommissionFailedException(decommissionAttribute, "invalid awareness attribute requested for decommissioning");
        }
        // TODO - should attribute value be part of force zone values? If yes, read setting and throw exception if not found
    }

    private static void ensureNoAwarenessAttributeDecommissioned(
        DecommissionAttributeMetadata decommissionAttributeMetadata,
        DecommissionAttribute decommissionAttribute
    ) {
        // If the previous decommission request failed, we will allow the request to pass this check
        if (decommissionAttributeMetadata != null && !decommissionAttributeMetadata.status().equals(DecommissionStatus.DECOMMISSION_FAILED)) {
            throw new DecommissionFailedException(decommissionAttribute, "one awareness attribute already decommissioned, " +
                "recommission before triggering another decommission");
        }
    }

    private void failDecommissionedNodes(ClusterState state) {
        DecommissionAttributeMetadata decommissionAttributeMetadata = state.metadata().custom(DecommissionAttributeMetadata.TYPE);
        // TODO update the status check to DECOMMISSIONING once graceful decommission is implemented
        assert decommissionAttributeMetadata.status().equals(DecommissionStatus.INIT) : "unexpected status encountered while decommissioning nodes";
        DecommissionAttribute decommissionAttribute = decommissionAttributeMetadata.decommissionAttribute();
        List<DiscoveryNode> nodesToBeDecommissioned = new ArrayList<>();
        final Predicate<DiscoveryNode> shouldRemoveNodePredicate = discoveryNode -> nodeHasDecommissionedAttribute(discoveryNode, decommissionAttribute);
        Iterator<DiscoveryNode> nodesIter = state.nodes().getNodes().valuesIt();
        while (nodesIter.hasNext()) {
            final DiscoveryNode node = nodesIter.next();
            if (shouldRemoveNodePredicate.test(node)) {
                nodesToBeDecommissioned.add(node);
            }
        }
        decommissionHelper.handleNodesDecommissionRequest(nodesToBeDecommissioned, "nodes-decommissioned");
    }

    private static boolean nodeHasDecommissionedAttribute(DiscoveryNode discoveryNode, DecommissionAttribute decommissionAttribute) {
        return discoveryNode.getAttributes().get(
            decommissionAttribute.attributeName()
        ).equals(decommissionAttribute.attributeValue());
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        clusterState = event.state();
    }
}

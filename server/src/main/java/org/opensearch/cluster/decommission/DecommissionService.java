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
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateApplier;
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
 *
 * Whenever a cluster manager initiates operation to decommission an awareness attribute,
 * the service makes the best attempt to perform the following task -
 * <p>
 * 1. Remove cluster-manager eligible nodes from voting config [TODO - checks to avoid quorum loss scenarios]
 * 2. Initiates nodes decommissioning by adding custom metadata with the attribute and state as {@link DecommissionStatus#DECOMMISSION_INIT}
 * 3. Triggers weigh away for nodes having given awareness attribute to drain. This marks the decommission status as {@link DecommissionStatus#DECOMMISSION_IN_PROGRESS}
 * 4. Once weighed away, the service triggers nodes decommission
 * 5. Once the decommission is successful, the service clears the voting config and marks the status as {@link DecommissionStatus#DECOMMISSION_SUCCESSFUL}
 * 6. If service fails at any step, it would mark the status as {@link DecommissionStatus#DECOMMISSION_FAILED}
 * </p>
 *
 * @opensearch.internal
 */
public class DecommissionService implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(DecommissionService.class);

    private final ClusterService clusterService;
    private final TransportService transportService;
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
        this.decommissionHelper = new DecommissionHelper(clusterService, allocationService);
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
        final ActionListener<ClusterStateUpdateResponse> listener,
        ClusterState state
    ) {
        validateAwarenessAttribute(decommissionAttribute, getAwarenessAttributes());
        this.clusterState = state;
        logger.info("initiating awareness attribute [{}] decommissioning", decommissionAttribute.toString());
        excludeDecommissionedClusterManagerNodesFromVotingConfig(decommissionAttribute, listener);
    }

    private void excludeDecommissionedClusterManagerNodesFromVotingConfig(
        DecommissionAttribute decommissionAttribute,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        final Predicate<DiscoveryNode> shouldDecommissionPredicate = discoveryNode -> nodeHasDecommissionedAttribute(
            discoveryNode,
            decommissionAttribute
        );
        List<String> clusterManagerNodesToBeDecommissioned = new ArrayList<>();
        Iterator<DiscoveryNode> clusterManagerNodesIter = clusterState.nodes().getClusterManagerNodes().valuesIt();
        while (clusterManagerNodesIter.hasNext()) {
            final DiscoveryNode node = clusterManagerNodesIter.next();
            if (shouldDecommissionPredicate.test(node)) {
                clusterManagerNodesToBeDecommissioned.add(node.getName());
            }
        }
        transportService.sendRequest(
            transportService.getLocalNode(),
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest(clusterManagerNodesToBeDecommissioned.toArray(String[]::new)),
            new TransportResponseHandler<AddVotingConfigExclusionsResponse>() {
                @Override
                public void handleResponse(AddVotingConfigExclusionsResponse response) {
                    logger.info(
                        "successfully removed decommissioned cluster manager eligible nodes [{}] from voting config, "
                            + "proceeding to drain the decommissioned nodes",
                        clusterManagerNodesToBeDecommissioned.toString()
                    );
                    registerDecommissionAttribute(decommissionAttribute, listener);
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.debug(
                        new ParameterizedMessage("failure in removing decommissioned cluster manager eligible nodes from voting config"),
                        exp
                    );
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public AddVotingConfigExclusionsResponse read(StreamInput in) throws IOException {
                    return new AddVotingConfigExclusionsResponse(in);
                }
            }
        );
    }

    private void clearVotingConfigAfterSuccessfulDecommission() {
        final ClearVotingConfigExclusionsRequest clearVotingConfigExclusionsRequest = new ClearVotingConfigExclusionsRequest();
        clearVotingConfigExclusionsRequest.setWaitForRemoval(true);
        transportService.sendRequest(
            transportService.getLocalNode(),
            ClearVotingConfigExclusionsAction.NAME,
            clearVotingConfigExclusionsRequest,
            new TransportResponseHandler<ClearVotingConfigExclusionsResponse>() {
                @Override
                public void handleResponse(ClearVotingConfigExclusionsResponse response) {
                    logger.info("successfully cleared voting config after decommissioning");
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.debug(new ParameterizedMessage("failure in clearing voting config exclusion after decommissioning"), exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public ClearVotingConfigExclusionsResponse read(StreamInput in) throws IOException {
                    return new ClearVotingConfigExclusionsResponse(in);
                }
            }
        );
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
        // check the local node is master and not in decommission attribute
        assert transportService.getLocalNode().isClusterManagerNode()
            && !nodeHasDecommissionedAttribute(transportService.getLocalNode(), decommissionAttribute)
            : "cannot register decommission attribute, as local node is not master or is going to be decommissioned";

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
                    // TODO - should we put the weights back to zone, since we weighed away the zone before we started registering the
                    // metadata
                    // TODO - should we modify logic of logging for ease of debugging?
                    if (e instanceof DecommissionFailedException) {
                        logger.error(
                            () -> new ParameterizedMessage("failed to decommission attribute [{}]", decommissionAttribute.toString()),
                            e
                        );
                    } else if (e instanceof NotClusterManagerException) {
                        logger.info(
                            () -> new ParameterizedMessage(
                                "cluster-manager updated while executing request for decommission attribute [{}]",
                                decommissionAttribute.toString()
                            ),
                            e
                        );
                    } else {
                        // could be due to on longer cluster manager
                        clusterService.submitStateUpdateTask("decommission_failed", new ClusterStateUpdateTask(Priority.URGENT) {
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
                                logger.error(
                                    () -> new ParameterizedMessage(
                                        "failed to mark status as DECOMMISSION_FAILED for decommission attribute [{}]",
                                        decommissionAttribute.toString()
                                    ),
                                    e
                                );
                                // listener.onFailure(e);
                            }
                        });
                    }
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    assert !newState.equals(oldState) : "no update in cluster state after initiating decommission request.";
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

    // To Do - Can we add a consumer here such that whenever this succeeds we call the next method in on cluster state processed
    private void updateMetadataWithDecommissionStatus(
        DecommissionStatus decommissionStatus
    ) {
        clusterService.submitStateUpdateTask(
            decommissionStatus.status(),
            new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    Metadata metadata = currentState.metadata();
                    DecommissionAttributeMetadata decommissionAttributeMetadata = metadata.custom(DecommissionAttributeMetadata.TYPE);
                    assert decommissionAttributeMetadata != null
                        && decommissionAttributeMetadata.decommissionAttribute() != null
                        : "failed to update status for decommission. metadata doesn't exist or invalid";
                    Metadata.Builder mdBuilder = Metadata.builder(metadata);
                    DecommissionAttributeMetadata newMetadata = decommissionAttributeMetadata.withUpdatedStatus(decommissionStatus);
                    mdBuilder.putCustom(DecommissionAttributeMetadata.TYPE, newMetadata);
                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error(
                        () -> new ParameterizedMessage(
                            "failed to mark status as [{}]",
                            decommissionStatus.status()
                        ),
                        e
                    );
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
        if (decommissionAttributeMetadata != null
            && !decommissionAttributeMetadata.status().equals(DecommissionStatus.DECOMMISSION_FAILED)) {
            throw new DecommissionFailedException(
                decommissionAttribute,
                "one awareness attribute already decommissioned, " + "recommission before triggering another decommission"
            );
        }
    }

    private void failDecommissionedNodes(ClusterState state) {
        DecommissionAttributeMetadata decommissionAttributeMetadata = state.metadata().custom(DecommissionAttributeMetadata.TYPE);
        // TODO update the status check to DECOMMISSIONING once graceful decommission is implemented
        assert decommissionAttributeMetadata.status().equals(DecommissionStatus.DECOMMISSION_INIT)
            : "unexpected status encountered while decommissioning nodes";
        DecommissionAttribute decommissionAttribute = decommissionAttributeMetadata.decommissionAttribute();
        List<DiscoveryNode> nodesToBeDecommissioned = new ArrayList<>();
        final Predicate<DiscoveryNode> shouldRemoveNodePredicate = discoveryNode -> nodeHasDecommissionedAttribute(
            discoveryNode,
            decommissionAttribute
        );
        Iterator<DiscoveryNode> nodesIter = state.nodes().getNodes().valuesIt();
        while (nodesIter.hasNext()) {
            final DiscoveryNode node = nodesIter.next();
            if (shouldRemoveNodePredicate.test(node)) {
                nodesToBeDecommissioned.add(node);
            }
        }
        // TODO - check for response from decommission request and then clear voting config?
        decommissionHelper.handleNodesDecommissionRequest(nodesToBeDecommissioned, "nodes-decommissioned");
        clearVotingConfigAfterSuccessfulDecommission();
    }

    private static boolean nodeHasDecommissionedAttribute(DiscoveryNode discoveryNode, DecommissionAttribute decommissionAttribute) {
        return discoveryNode.getAttributes().get(decommissionAttribute.attributeName()).equals(decommissionAttribute.attributeValue());
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        clusterState = event.state();
    }
}

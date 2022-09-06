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
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING;
import static org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING;

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
    private final TransportService transportService;
    private final ThreadPool threadPool;
    private final DecommissionController decommissionController;
    private volatile List<String> awarenessAttributes;
    private volatile Map<String, List<String>> forcedAwarenessAttributes;

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
        this.decommissionController = new DecommissionController(clusterService, transportService, allocationService, threadPool);
        this.awarenessAttributes = CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING, this::setAwarenessAttributes);

        setForcedAwarenessAttributes(CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING,
            this::setForcedAwarenessAttributes
        );
    }

    private void setAwarenessAttributes(List<String> awarenessAttributes) {
        this.awarenessAttributes = awarenessAttributes;
    }

    private void setForcedAwarenessAttributes(Settings forceSettings) {
        Map<String, List<String>> forcedAwarenessAttributes = new HashMap<>();
        Map<String, Settings> forceGroups = forceSettings.getAsGroups();
        for (Map.Entry<String, Settings> entry : forceGroups.entrySet()) {
            List<String> aValues = entry.getValue().getAsList("values");
            if (aValues.size() > 0) {
                forcedAwarenessAttributes.put(entry.getKey(), aValues);
            }
        }
        this.forcedAwarenessAttributes = forcedAwarenessAttributes;
    }

    public void initiateAttributeDecommissioning(
        final DecommissionAttribute decommissionAttribute,
        final ActionListener<ClusterStateUpdateResponse> listener,
        ClusterState state
    ) {
        // validates if the correct awareness attributes and forced awareness attribute set to the cluster before initiating decommission
        // action
        validateAwarenessAttribute(decommissionAttribute, awarenessAttributes, forcedAwarenessAttributes);

        Set<DiscoveryNode> clusterManagerNodesToBeDecommissioned = filterNodesWithDecommissionAttribute(state, decommissionAttribute, true);
        ensureNoQuorumLossDueToDecommissioning(
            decommissionAttribute,
            clusterManagerNodesToBeDecommissioned,
            state.getLastAcceptedConfiguration()
        );

        DecommissionAttributeMetadata decommissionAttributeMetadata = state.metadata().custom(DecommissionAttributeMetadata.TYPE);
        // validates that there's no inflight decommissioning or already executed decommission in place
        ensureNoInflightDifferentDecommissionRequest(decommissionAttributeMetadata, decommissionAttribute);

        logger.info("initiating awareness attribute [{}] decommissioning", decommissionAttribute.toString());

        // remove all 'to-be-decommissioned' cluster manager eligible nodes from voting config
        // The method ensures that we don't exclude same nodes multiple times
        excludeDecommissionedClusterManagerNodesFromVotingConfig(clusterManagerNodesToBeDecommissioned);

        // explicitly throwing NotClusterManagerException as we can certainly say the local cluster manager node will
        // be abdicated and soon will no longer be cluster manager.
        if (transportService.getLocalNode().isClusterManagerNode()
            && !nodeHasDecommissionedAttribute(transportService.getLocalNode(), decommissionAttribute)) {
            registerDecommissionAttribute(decommissionAttribute, listener);
        } else {
            throw new NotClusterManagerException(
                "node ["
                    + transportService.getLocalNode().toString()
                    + "] not eligible to execute decommission request. Will retry until timeout."
            );
        }
    }

    private void excludeDecommissionedClusterManagerNodesFromVotingConfig(Set<DiscoveryNode> clusterManagerNodesToBeDecommissioned) {
        Set<String> clusterManagerNodesNameToBeDecommissioned = clusterManagerNodesToBeDecommissioned.stream()
            .map(DiscoveryNode::getName)
            .collect(Collectors.toSet());

        Set<VotingConfigExclusion> currentVotingConfigExclusions = clusterService.getClusterApplierService()
            .state()
            .coordinationMetadata()
            .getVotingConfigExclusions();
        Set<String> excludedNodesName = currentVotingConfigExclusions.stream()
            .map(VotingConfigExclusion::getNodeName)
            .collect(Collectors.toSet());

        // check if the to-be-excluded nodes are excluded. If yes, we don't need to exclude them again
        if (clusterManagerNodesNameToBeDecommissioned.size() == 0
            || (clusterManagerNodesNameToBeDecommissioned.size() == excludedNodesName.size()
                && excludedNodesName.containsAll(clusterManagerNodesNameToBeDecommissioned))) {
            return;
        }
        // send a transport request to exclude to-be-decommissioned cluster manager eligible nodes from voting config
        decommissionController.excludeDecommissionedNodesFromVotingConfig(
            clusterManagerNodesNameToBeDecommissioned,
            new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    logger.info(
                        "successfully removed decommissioned cluster manager eligible nodes [{}] from voting config ",
                        clusterManagerNodesToBeDecommissioned.toString()
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug(
                        new ParameterizedMessage("failure in removing decommissioned cluster manager eligible nodes from voting config"),
                        e
                    );
                }
            }
        );
    }

    /**
     * Registers new decommissioned attribute metadata in the cluster state with {@link DecommissionStatus#DECOMMISSION_INIT}
     * <p>
     * This method can be only called on the cluster-manager node. It tries to create a new decommissioned attribute on the cluster manager
     * and if it was successful it adds new decommissioned attribute to cluster metadata.
     * <p>
     * This method would only be executed on eligible cluster manager node
     *
     * @param decommissionAttribute register decommission attribute in the metadata request
     * @param listener              register decommission listener
     */
    private void registerDecommissionAttribute(
        final DecommissionAttribute decommissionAttribute,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        clusterService.submitStateUpdateTask(
            "put_decommission [" + decommissionAttribute + "]",
            new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    Metadata metadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(metadata);
                    DecommissionAttributeMetadata decommissionAttributeMetadata = metadata.custom(DecommissionAttributeMetadata.TYPE);
                    ensureNoInflightDifferentDecommissionRequest(decommissionAttributeMetadata, decommissionAttribute);
                    decommissionAttributeMetadata = new DecommissionAttributeMetadata(decommissionAttribute);
                    mdBuilder.putCustom(DecommissionAttributeMetadata.TYPE, decommissionAttributeMetadata);
                    logger.info(
                        "registering decommission metadata for attribute [{}] with status as [{}]",
                        decommissionAttribute.toString(),
                        DecommissionStatus.DECOMMISSION_INIT
                    );
                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    if (e instanceof NotClusterManagerException) {
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "cluster-manager updated while executing request for decommission attribute [{}]",
                                decommissionAttribute.toString()
                            ),
                            e
                        );
                        // we don't want to send the failure response to the listener here as the request will be retried
                    } else {
                        logger.error(
                            () -> new ParameterizedMessage(
                                "failed to initiate decommissioning for attribute [{}]",
                                decommissionAttribute.toString()
                            ),
                            e
                        );
                        failAndClearVotingConfigExclusion(listener, e);
                    }
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    DecommissionAttributeMetadata decommissionAttributeMetadata = newState.metadata()
                        .custom(DecommissionAttributeMetadata.TYPE);
                    assert decommissionAttribute.equals(decommissionAttributeMetadata.decommissionAttribute());
                    assert DecommissionStatus.DECOMMISSION_INIT.equals(decommissionAttributeMetadata.status());
                    listener.onResponse(new ClusterStateUpdateResponse(true));
                    weighAwayForGracefulDecommission();
                }
            }
        );
    }

    private void failAndClearVotingConfigExclusion(final ActionListener<ClusterStateUpdateResponse> listener, Exception e) {
        decommissionController.clearVotingConfigExclusion(new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                logger.info("successfully cleared voting config exclusion after failing to execute decommission request");
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug(
                    new ParameterizedMessage("failure in clearing voting config exclusion after failing to execute decommission request"),
                    e
                );
            }
        });
        listener.onFailure(e);
    }

    private void weighAwayForGracefulDecommission() {
        decommissionController.updateMetadataWithDecommissionStatus(
            DecommissionStatus.DECOMMISSION_IN_PROGRESS,
            new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    logger.info(
                        "updated decommission status to [{}], weighing away awareness attribute for graceful shutdown",
                        DecommissionStatus.DECOMMISSION_IN_PROGRESS
                    );
                    // TODO - should trigger weigh away here and on successful weigh away -> fail the decommissioned nodes
                    failDecommissionedNodes(clusterService.getClusterApplierService().state());
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(
                        () -> new ParameterizedMessage(
                            "failed to update decommission status to [{}], will not proceed with decommission",
                            DecommissionStatus.DECOMMISSION_IN_PROGRESS
                        ),
                        e
                    );
                }
            }
        );
    }

    private void failDecommissionedNodes(ClusterState state) {
        DecommissionAttributeMetadata decommissionAttributeMetadata = state.metadata().custom(DecommissionAttributeMetadata.TYPE);
        assert decommissionAttributeMetadata.status().equals(DecommissionStatus.DECOMMISSION_IN_PROGRESS)
            : "unexpected status encountered while decommissioning nodes";
        DecommissionAttribute decommissionAttribute = decommissionAttributeMetadata.decommissionAttribute();

        // execute nodes decommissioning
        decommissionController.removeDecommissionedNodes(
            filterNodesWithDecommissionAttribute(state, decommissionAttribute, false),
            "nodes-decommissioned",
            TimeValue.timeValueSeconds(30L), // TODO - read timeout from request while integrating with API
            new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    clearVotingConfigExclusionAndUpdateStatus(true);
                }

                @Override
                public void onFailure(Exception e) {
                    clearVotingConfigExclusionAndUpdateStatus(false);
                }
            }
        );
    }

    private void clearVotingConfigExclusionAndUpdateStatus(boolean decommissionSuccessful) {
        ActionListener<Void> statusUpdateListener = new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                logger.info(
                    "successful updated decommission status with [{}]",
                    decommissionSuccessful ? DecommissionStatus.DECOMMISSION_SUCCESSFUL : DecommissionStatus.DECOMMISSION_FAILED
                );
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("failed to update the decommission status");
            }
        };
        decommissionController.clearVotingConfigExclusion(new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                logger.info("successfully cleared voting config exclusion after failing to execute decommission request");
                DecommissionStatus updateStatusWith = decommissionSuccessful
                    ? DecommissionStatus.DECOMMISSION_SUCCESSFUL
                    : DecommissionStatus.DECOMMISSION_FAILED;
                decommissionController.updateMetadataWithDecommissionStatus(updateStatusWith, statusUpdateListener);
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug(
                    new ParameterizedMessage("failure in clearing voting config exclusion after processing decommission request"),
                    e
                );
                decommissionController.updateMetadataWithDecommissionStatus(DecommissionStatus.DECOMMISSION_FAILED, statusUpdateListener);
            }
        });
    }

    private Set<DiscoveryNode> filterNodesWithDecommissionAttribute(
        ClusterState clusterState,
        DecommissionAttribute decommissionAttribute,
        boolean onlyClusterManagerNodes
    ) {
        Set<DiscoveryNode> nodesWithDecommissionAttribute = new HashSet<>();
        Iterator<DiscoveryNode> nodesIter = onlyClusterManagerNodes
            ? clusterState.nodes().getClusterManagerNodes().valuesIt()
            : clusterState.nodes().getNodes().valuesIt();

        while (nodesIter.hasNext()) {
            final DiscoveryNode node = nodesIter.next();
            if (nodeHasDecommissionedAttribute(node, decommissionAttribute)) {
                nodesWithDecommissionAttribute.add(node);
            }
        }
        return nodesWithDecommissionAttribute;
    }

    private static boolean nodeHasDecommissionedAttribute(DiscoveryNode discoveryNode, DecommissionAttribute decommissionAttribute) {
        return discoveryNode.getAttributes().get(decommissionAttribute.attributeName()).equals(decommissionAttribute.attributeValue());
    }

    private static void validateAwarenessAttribute(
        final DecommissionAttribute decommissionAttribute,
        List<String> awarenessAttributes,
        Map<String, List<String>> forcedAwarenessAttributes
    ) {
        String msg = null;
        if (awarenessAttributes == null) {
            msg = "awareness attribute not set to the cluster.";
        } else if (forcedAwarenessAttributes == null) {
            msg = "forced awareness attribute not set to the cluster.";
        } else if (!awarenessAttributes.contains(decommissionAttribute.attributeName())) {
            msg = "invalid awareness attribute requested for decommissioning";
        } else if (!forcedAwarenessAttributes.containsKey(decommissionAttribute.attributeName())) {
            msg = "forced awareness attribute [" + forcedAwarenessAttributes.toString() + "] doesn't have the decommissioning attribute";
        } else if (!forcedAwarenessAttributes.get(decommissionAttribute.attributeName()).contains(decommissionAttribute.attributeValue())) {
            msg = "invalid awareness attribute value requested for decommissioning. Set forced awareness values before to decommission";
        }

        if (msg != null) {
            throw new DecommissioningFailedException(decommissionAttribute, msg);
        }
    }

    private static void ensureNoInflightDifferentDecommissionRequest(
        DecommissionAttributeMetadata decommissionAttributeMetadata,
        DecommissionAttribute decommissionAttribute
    ) {
        String msg = null;
        if (decommissionAttributeMetadata != null) {
            if (decommissionAttributeMetadata.status().equals(DecommissionStatus.DECOMMISSION_SUCCESSFUL)) {
                // one awareness attribute is already decommissioned. We will reject the new request
                msg = "one awareness attribute already successfully decommissioned. Recommission before triggering another decommission";
            } else if (decommissionAttributeMetadata.status().equals(DecommissionStatus.DECOMMISSION_FAILED)) {
                // here we are sure that the previous decommission request failed, we can let this request pass this check
                return;
            } else {
                // it means the decommission has been initiated or is inflight. In that case, if the same attribute is requested for
                // decommissioning, which can happen during retries, we will pass this check, if not, we will throw exception
                if (!decommissionAttributeMetadata.decommissionAttribute().equals(decommissionAttribute)) {
                    msg = "another request for decommission is in flight, will not process this request";
                }
            }
        }
        if (msg != null) {
            throw new DecommissioningFailedException(decommissionAttribute, msg);
        }
    }

    private static void ensureNoQuorumLossDueToDecommissioning(
        DecommissionAttribute decommissionAttribute,
        Set<DiscoveryNode> clusterManagerNodesToBeDecommissioned,
        CoordinationMetadata.VotingConfiguration votingConfiguration
    ) {
        Set<String> clusterManagerNodesIdToBeDecommissioned = new HashSet<>();
        clusterManagerNodesToBeDecommissioned.forEach(node -> clusterManagerNodesIdToBeDecommissioned.add(node.getId()));
        if (!votingConfiguration.hasQuorum(
            votingConfiguration.getNodeIds()
                .stream()
                .filter(n -> clusterManagerNodesIdToBeDecommissioned.contains(n) == false)
                .collect(Collectors.toList())
        )) {
            throw new DecommissioningFailedException(
                decommissionAttribute,
                "cannot proceed with decommission request as cluster might go into quorum loss"
            );
        }
    }
}

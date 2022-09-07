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
import org.opensearch.cluster.ClusterStateObserver;
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
 * <li>Initiates nodes decommissioning by adding custom metadata with the attribute and state as {@link DecommissionStatus#INIT}</li>
 * <li>Remove cluster-manager eligible nodes from voting config </li>
 * <li>Triggers weigh away for nodes having given awareness attribute to drain. This marks the decommission status as {@link DecommissionStatus#IN_PROGRESS}</li>
 * <li>Once weighed away, the service triggers nodes decommission</li>
 * <li>Once the decommission is successful, the service clears the voting config and marks the status as {@link DecommissionStatus#SUCCESSFUL}</li>
 * <li>If service fails at any step, it would mark the status as {@link DecommissionStatus#FAILED}</li>
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

    /**
     * Starts the new decommission request and registers the metadata with status as {@link DecommissionStatus#INIT}
     * or the last known status if not {@link DecommissionStatus#FAILED}
     * Once the status is updated, it tries to exclude to-be-decommissioned cluster manager nodes from Voting Configuration
     *
     * @param decommissionAttribute register decommission attribute in the metadata request
     * @param listener register decommission listener
     */
    public synchronized void startDecommissionAction(
        final DecommissionAttribute decommissionAttribute,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        // validates if correct awareness attributes and forced awareness attribute set to the cluster before starting action
        validateAwarenessAttribute(decommissionAttribute, awarenessAttributes, forcedAwarenessAttributes);

        // register the metadata with status as DECOMMISSION_INIT as first step
        clusterService.submitStateUpdateTask(
            "decommission [" + decommissionAttribute + "]",
            new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    Metadata metadata = currentState.metadata();
                    Metadata.Builder mdBuilder = Metadata.builder(metadata);
                    DecommissionAttributeMetadata decommissionAttributeMetadata = metadata.custom(DecommissionAttributeMetadata.TYPE);
                    // check the request sanity and reject the request if there's any inflight or successful request already present
                    ensureNoInflightDifferentDecommissionRequest(decommissionAttributeMetadata, decommissionAttribute);
                    // check if the same attribute is requested for decommission and currently not FAILED, then return the current state as is
                    if(decommissionAttributeMetadata!=null &&
                        decommissionAttributeMetadata.decommissionAttribute().equals(decommissionAttribute) &&
                        !decommissionAttributeMetadata.status().equals(DecommissionStatus.FAILED)) {
                        logger.info("re-request received for decommissioning [{}], will not update state", decommissionAttribute);
                        return currentState;
                    }
                    decommissionAttributeMetadata = new DecommissionAttributeMetadata(decommissionAttribute);
                    mdBuilder.putCustom(DecommissionAttributeMetadata.TYPE, decommissionAttributeMetadata);
                    logger.info(
                        "registering decommission metadata [{}] to execute action",
                        decommissionAttributeMetadata.toString()
                    );
                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error(
                        () -> new ParameterizedMessage(
                            "failed to start decommission action for attribute [{}]",
                            decommissionAttribute.toString()
                        ),
                        e
                    );
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    DecommissionAttributeMetadata decommissionAttributeMetadata = newState.metadata()
                        .custom(DecommissionAttributeMetadata.TYPE);
                    assert decommissionAttribute.equals(decommissionAttributeMetadata.decommissionAttribute());
                    decommissionClusterManagerNodes(
                        decommissionAttributeMetadata.decommissionAttribute(),
                        listener
                    );
                }
            }
        );
    }

    private void decommissionClusterManagerNodes(
        final DecommissionAttribute decommissionAttribute,
        ActionListener<ClusterStateUpdateResponse> listener
    ) {
        ClusterState state = clusterService.getClusterApplierService().state();
        Set<DiscoveryNode> clusterManagerNodesToBeDecommissioned = filterNodesWithDecommissionAttribute(
            state, decommissionAttribute, true
        );
        try {
            // this is a sanity check that the cluster will not go into a quorum loss state because of exclusion
            ensureNoQuorumLossDueToDecommissioning(decommissionAttribute, clusterManagerNodesToBeDecommissioned, state.getLastCommittedConfiguration());
        } catch (DecommissioningFailedException dfe) {
            listener.onFailure(dfe);
            decommissionController.updateMetadataWithDecommissionStatus(DecommissionStatus.FAILED, new ActionListener<DecommissionStatus>() {
                @Override
                public void onResponse(DecommissionStatus status) {
                    logger.info("updated the status to [{}], as cluster could have gone to quorum loss situation due to decommissioning", status.toString());
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("unexpected error found while updating the status", e);
                }
            });
        }
        // remove all 'to-be-decommissioned' cluster manager eligible nodes from voting config
        // The method ensures that we don't exclude same nodes multiple times
        boolean toBeDecommissionedClusterManagerNodesExcluded = excludeDecommissionedClusterManagerNodesFromVotingConfig(clusterManagerNodesToBeDecommissioned);

        if (transportService.getLocalNode().isClusterManagerNode()
            && !nodeHasDecommissionedAttribute(transportService.getLocalNode(), decommissionAttribute)
            && toBeDecommissionedClusterManagerNodesExcluded
        ) {
            // we are good here to send the response now as the request is processed by an eligible active leader
            // and to-be-decommissioned cluster manager is no more part of Voting Configuration
            listener.onResponse(new ClusterStateUpdateResponse(true));
            failDecommissionedNodes(clusterService.getClusterApplierService().state());
        } else {
            // explicitly calling listener.onFailure with NotClusterManagerException as we can certainly say the local cluster manager node will
            // be abdicated and soon will no longer be cluster manager.
            // this will ensure that request is retried until cluster manager times out
            listener.onFailure(new NotClusterManagerException("node ["
                + transportService.getLocalNode().toString()
                + "] not eligible to execute decommission request. Will retry until timeout."));
        }
    }

    private boolean excludeDecommissionedClusterManagerNodesFromVotingConfig(Set<DiscoveryNode> clusterManagerNodesToBeDecommissioned) {
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
        if (clusterManagerNodesNameToBeDecommissioned.size() == 0 || excludedNodesName.containsAll(clusterManagerNodesNameToBeDecommissioned)) {
            return true;
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
        // send false for now and let the transport request be retried
        return false;
    }

    private void failDecommissionedNodes(ClusterState state) {
        // this method ensures no matter what, we always exit from this function after clearing the voting config exclusion
        DecommissionAttributeMetadata decommissionAttributeMetadata = state.metadata().custom(DecommissionAttributeMetadata.TYPE);
        DecommissionAttribute decommissionAttribute = decommissionAttributeMetadata.decommissionAttribute();
        decommissionController.updateMetadataWithDecommissionStatus(
            DecommissionStatus.IN_PROGRESS,
            new ActionListener<>() {
                @Override
                public void onResponse(DecommissionStatus status) {
                    // execute nodes decommissioning
                    decommissionController.removeDecommissionedNodes(
                        filterNodesWithDecommissionAttribute(
                            clusterService.getClusterApplierService().state(), decommissionAttribute, false
                        ),
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

                @Override
                public void onFailure(Exception e) {
                    logger.error(
                        () -> new ParameterizedMessage(
                            "failed to update decommission status for attribute [{}] to [{}]",
                            decommissionAttribute.toString(),
                            DecommissionStatus.IN_PROGRESS
                        ),
                        e
                    );
                    // since we are not able to update the status, we will clear the voting config exclusion we have set earlier
                    clearVotingConfigExclusionAndUpdateStatus(false);
                }
            }
        );
    }

    private void clearVotingConfigExclusionAndUpdateStatus(boolean decommissionSuccessful) {
        ActionListener<DecommissionStatus> statusUpdateListener = new ActionListener<>() {
            @Override
            public void onResponse(DecommissionStatus status) {
                logger.info("completed decommission action");
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("failure encountered while executing decommission action");
            }
        };
        decommissionController.clearVotingConfigExclusion(new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                logger.info("successfully cleared voting config exclusion after completing decommission action, proceeding to update metadata");
                DecommissionStatus updateStatusWith = decommissionSuccessful
                    ? DecommissionStatus.SUCCESSFUL
                    : DecommissionStatus.FAILED;
                decommissionController.updateMetadataWithDecommissionStatus(updateStatusWith, statusUpdateListener);
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug(
                    new ParameterizedMessage("failure in clearing voting config exclusion after processing decommission request"),
                    e
                );
                decommissionController.updateMetadataWithDecommissionStatus(DecommissionStatus.FAILED, statusUpdateListener);
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
            if (decommissionAttributeMetadata.status().equals(DecommissionStatus.SUCCESSFUL)) {
                // one awareness attribute is already decommissioned. We will reject the new request
                msg = "one awareness attribute already successfully decommissioned. Recommission before triggering another decommission";
            } else if (decommissionAttributeMetadata.status().equals(DecommissionStatus.FAILED)) {
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

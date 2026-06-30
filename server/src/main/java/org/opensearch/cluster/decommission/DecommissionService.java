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
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateResponse;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionRequest;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.ClusterStateObserver.Listener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.WeightedRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.UUIDs;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.opensearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction.MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING;
import static org.opensearch.action.admin.cluster.configuration.VotingConfigExclusionsHelper.clearExclusionsAndGetState;
import static org.opensearch.cluster.decommission.DecommissionHelper.addVotingConfigExclusionsForNodesToBeDecommissioned;
import static org.opensearch.cluster.decommission.DecommissionHelper.deleteDecommissionAttributeInClusterState;
import static org.opensearch.cluster.decommission.DecommissionHelper.filterNodesWithDecommissionAttribute;
import static org.opensearch.cluster.decommission.DecommissionHelper.nodeHasDecommissionedAttribute;
import static org.opensearch.cluster.decommission.DecommissionHelper.registerDecommissionAttributeInClusterState;
import static org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING;
import static org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING;

/**
 * Service responsible for entire lifecycle of decommissioning and recommissioning an awareness attribute.
 * <p>
 * Whenever a cluster manager initiates operation to decommission an awareness attribute,
 * the service makes the best attempt to perform the following task -
 * <ul>
 * <li>Initiates nodes decommissioning by adding custom metadata with the attribute and state as {@link DecommissionStatus#INIT}</li>
 * <li>Remove to-be-decommissioned cluster-manager eligible nodes from voting config and wait for its abdication if it is active leader</li>
 * <li>After the draining timeout, the service triggers nodes decommission. This marks the decommission status as {@link DecommissionStatus#IN_PROGRESS}</li>
 * <li>Once the decommission is successful, the service clears the voting config and marks the status as {@link DecommissionStatus#SUCCESSFUL}</li>
 * <li>If service fails at any step, it makes best attempt to mark the status as {@link DecommissionStatus#FAILED} and to clear voting config exclusion</li>
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
    private volatile int maxVotingConfigExclusions;

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
        maxVotingConfigExclusions = MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING, this::setMaxVotingConfigExclusions);
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

    private void setMaxVotingConfigExclusions(int maxVotingConfigExclusions) {
        this.maxVotingConfigExclusions = maxVotingConfigExclusions;
    }

    /**
     * Starts the new decommission request and registers the metadata with status as {@link DecommissionStatus#INIT}
     * Once the status is updated, it tries to exclude to-be-decommissioned cluster manager eligible nodes from Voting Configuration
     *
     * @param decommissionRequest request for decommission action
     * @param listener register decommission listener
     */
    public void startDecommissionAction(
        final DecommissionRequest decommissionRequest,
        final ActionListener<DecommissionResponse> listener
    ) {
        final DecommissionAttribute decommissionAttribute = decommissionRequest.getDecommissionAttribute();
        // register the metadata with status as INIT as first step
        clusterService.submitStateUpdateTask("decommission [" + decommissionAttribute + "]", new ClusterStateUpdateTask(Priority.URGENT) {
            private Set<String> nodeIdsToBeExcluded;

            @Override
            public ClusterState execute(ClusterState currentState) {
                // validates if correct awareness attributes and forced awareness attribute set to the cluster before starting action
                validateAwarenessAttribute(decommissionAttribute, awarenessAttributes, forcedAwarenessAttributes);
                if (decommissionRequest.requestID() == null) {
                    decommissionRequest.setRequestID(UUIDs.randomBase64UUID());
                }
                DecommissionAttributeMetadata decommissionAttributeMetadata = currentState.metadata().decommissionAttributeMetadata();
                // check that request is eligible to proceed and attribute is weighed away
                ensureEligibleRequest(decommissionAttributeMetadata, decommissionRequest);
                ensureToBeDecommissionedAttributeWeighedAway(currentState, decommissionAttribute);

                ClusterState newState = registerDecommissionAttributeInClusterState(
                    currentState,
                    decommissionAttribute,
                    decommissionRequest.requestID()
                );
                // add all 'to-be-decommissioned' cluster manager eligible nodes to voting config exclusion
                nodeIdsToBeExcluded = filterNodesWithDecommissionAttribute(currentState, decommissionAttribute, true).stream()
                    .map(DiscoveryNode::getId)
                    .collect(Collectors.toSet());
                logger.info(
                    "resolved cluster manager eligible nodes [{}] that should be added to voting config exclusion",
                    nodeIdsToBeExcluded.toString()
                );
                newState = addVotingConfigExclusionsForNodesToBeDecommissioned(
                    newState,
                    nodeIdsToBeExcluded,
                    TimeValue.timeValueSeconds(120), // TODO - update it with request timeout
                    maxVotingConfigExclusions
                );
                logger.debug(
                    "registering decommission metadata [{}] to execute action",
                    newState.metadata().decommissionAttributeMetadata().toString()
                );
                return newState;
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
                DecommissionAttributeMetadata decommissionAttributeMetadata = newState.metadata().decommissionAttributeMetadata();
                assert decommissionAttribute.equals(decommissionAttributeMetadata.decommissionAttribute());
                assert decommissionAttributeMetadata.status().equals(DecommissionStatus.INIT);
                assert decommissionAttributeMetadata.requestID().equals(decommissionRequest.requestID());
                assert newState.getVotingConfigExclusions()
                    .stream()
                    .map(CoordinationMetadata.VotingConfigExclusion::getNodeId)
                    .collect(Collectors.toSet())
                    .containsAll(nodeIdsToBeExcluded);
                logger.debug(
                    "registered decommission metadata for attribute [{}] with status [{}]",
                    decommissionAttributeMetadata.decommissionAttribute(),
                    decommissionAttributeMetadata.status()
                );

                final ClusterStateObserver observer = new ClusterStateObserver(
                    clusterService,
                    TimeValue.timeValueSeconds(120), // TODO - update it with request timeout
                    logger,
                    threadPool.getThreadContext()
                );

                final Predicate<ClusterState> allNodesRemovedAndAbdicated = clusterState -> {
                    final Set<String> votingConfigNodeIds = clusterState.getLastCommittedConfiguration().getNodeIds();
                    return nodeIdsToBeExcluded.stream().noneMatch(votingConfigNodeIds::contains)
                        && clusterState.nodes().getClusterManagerNodeId() != null
                        && nodeIdsToBeExcluded.contains(clusterState.nodes().getClusterManagerNodeId()) == false;
                };

                final Listener clusterStateListener = new Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        logger.info(
                            "successfully removed decommissioned cluster manager eligible nodes [{}] from voting config ",
                            nodeIdsToBeExcluded.toString()
                        );
                        if (state.nodes().isLocalNodeElectedClusterManager()) {
                            if (nodeHasDecommissionedAttribute(clusterService.localNode(), decommissionAttribute)) {
                                // this is an unexpected state, as after exclusion of nodes having decommission attribute,
                                // this local node shouldn't have had the decommission attribute. Will send the failure response to the user
                                String errorMsg =
                                    "unexpected state encountered [local node is to-be-decommissioned leader] while executing decommission request";
                                logger.error(errorMsg);
                                // will go ahead and clear the voting config and mark the status as failed
                                decommissionController.updateMetadataWithDecommissionStatus(
                                    DecommissionStatus.FAILED,
                                    statusUpdateListener()
                                );
                                listener.onFailure(new IllegalStateException(errorMsg));
                            } else {
                                logger.info("will proceed to drain decommissioned nodes as local node is eligible to process the request");
                                // we are good here to send the response now as the request is processed by an eligible active leader
                                // and to-be-decommissioned cluster manager is no more part of Voting Configuration
                                listener.onResponse(new DecommissionResponse(true));
                                drainNodesWithDecommissionedAttribute(decommissionRequest);
                            }
                        } else {
                            // explicitly calling listener.onFailure with NotClusterManagerException as the local node is not leader
                            // this will ensures that request is retried until cluster manager times out
                            logger.info(
                                "local node is not eligible to process the request, "
                                    + "throwing NotClusterManagerException to attempt a retry on an eligible node"
                            );
                            listener.onFailure(
                                new NotClusterManagerException(
                                    "node ["
                                        + transportService.getLocalNode().toString()
                                        + "] not eligible to execute decommission request. Will retry until timeout."
                                )
                            );
                        }
                    }

                    @Override
                    public void onClusterServiceClose() {
                        String errorMsg = "cluster service closed while waiting for abdication of to-be-decommissioned leader";
                        logger.error(errorMsg);
                        listener.onFailure(new DecommissioningFailedException(decommissionAttribute, errorMsg));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        String errorMsg = "timed out ["
                            + timeout.toString()
                            + "] while removing to-be-decommissioned cluster manager eligible nodes ["
                            + nodeIdsToBeExcluded.toString()
                            + "] from voting config";
                        logger.error(errorMsg);
                        listener.onFailure(new OpenSearchTimeoutException(errorMsg));
                        // will go ahead and clear the voting config and mark the status as failed
                        decommissionController.updateMetadataWithDecommissionStatus(DecommissionStatus.FAILED, statusUpdateListener());
                    }
                };

                // In case the cluster state is already processed even before this code is executed
                // therefore testing first before attaching the listener
                if (allNodesRemovedAndAbdicated.test(newState)) {
                    clusterStateListener.onNewClusterState(newState);
                } else {
                    logger.debug("waiting to abdicate to-be-decommissioned leader");
                    observer.waitForNextChange(clusterStateListener, allNodesRemovedAndAbdicated); // TODO add request timeout here
                }
            }
        });
    }

    // TODO - after registering the new status check if any node which is not excluded still present in decommissioned zone. If yes, start
    // the action again (retry)
    void drainNodesWithDecommissionedAttribute(DecommissionRequest decommissionRequest) {
        ClusterState state = clusterService.getClusterApplierService().state();
        assert state.metadata().decommissionAttributeMetadata().requestID().equals(decommissionRequest.requestID());
        Set<DiscoveryNode> decommissionedNodes = filterNodesWithDecommissionAttribute(
            state,
            decommissionRequest.getDecommissionAttribute(),
            false
        );

        if (decommissionRequest.isNoDelay()) {
            // Call to fail the decommission nodes
            failDecommissionedNodes(decommissionedNodes, decommissionRequest.getDecommissionAttribute());
        } else {
            decommissionController.updateMetadataWithDecommissionStatus(DecommissionStatus.DRAINING, new ActionListener<>() {
                @Override
                public void onResponse(DecommissionStatus status) {
                    logger.info("updated the decommission status to [{}]", status);
                    // set the weights
                    scheduleNodesDecommissionOnTimeout(decommissionedNodes, decommissionRequest.getDelayTimeout());
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(
                        () -> new ParameterizedMessage(
                            "failed to update decommission status for attribute [{}] to [{}]",
                            decommissionRequest.getDecommissionAttribute().toString(),
                            DecommissionStatus.DRAINING
                        ),
                        e
                    );
                    // This decommission state update call will most likely fail as the state update call to 'DRAINING'
                    // failed. But attempting it anyways as FAILED update might still pass as it doesn't have dependency on
                    // the current state
                    decommissionController.updateMetadataWithDecommissionStatus(DecommissionStatus.FAILED, statusUpdateListener());
                }
            });
        }
    }

    void scheduleNodesDecommissionOnTimeout(Set<DiscoveryNode> decommissionedNodes, TimeValue timeoutForNodeDraining) {
        ClusterState state = clusterService.getClusterApplierService().state();
        DecommissionAttributeMetadata decommissionAttributeMetadata = state.metadata().decommissionAttributeMetadata();
        if (decommissionAttributeMetadata == null) {
            return;
        }
        assert decommissionAttributeMetadata.status().equals(DecommissionStatus.DRAINING)
            : "Unexpected status encountered while decommissioning nodes.";

        // This method ensures no matter what, we always exit from this function after clearing the voting config exclusion
        DecommissionAttribute decommissionAttribute = decommissionAttributeMetadata.decommissionAttribute();

        // Wait for timeout to happen. Log the active connection before decommissioning of nodes.
        transportService.getThreadPool().schedule(() -> {
            // Log active connections.
            decommissionController.getActiveRequestCountOnDecommissionedNodes(decommissionedNodes);
            // Call to fail the decommission nodes
            failDecommissionedNodes(decommissionedNodes, decommissionAttribute);
        }, timeoutForNodeDraining, ThreadPool.Names.GENERIC);
    }

    private void failDecommissionedNodes(Set<DiscoveryNode> decommissionedNodes, DecommissionAttribute decommissionAttribute) {

        // Weighing away is complete. We have allowed the nodes to be drained. Let's move decommission status to IN_PROGRESS.
        decommissionController.updateMetadataWithDecommissionStatus(DecommissionStatus.IN_PROGRESS, new ActionListener<>() {
            @Override
            public void onResponse(DecommissionStatus status) {
                logger.info("updated the decommission status to [{}]", status);
                // execute nodes decommissioning
                decommissionController.removeDecommissionedNodes(
                    decommissionedNodes,
                    "nodes-decommissioned",
                    TimeValue.timeValueSeconds(120L),
                    new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void unused) {
                            // will clear the voting config exclusion and mark the status as successful
                            decommissionController.updateMetadataWithDecommissionStatus(
                                DecommissionStatus.SUCCESSFUL,
                                statusUpdateListener()
                            );
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // will go ahead and clear the voting config and mark the status as failed
                            decommissionController.updateMetadataWithDecommissionStatus(DecommissionStatus.FAILED, statusUpdateListener());
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
                // This decommission state update call will most likely fail as the state update call to 'DRAINING'
                // failed. But attempting it anyways as FAILED update might still pass as it doesn't have dependency on
                // the current state
                decommissionController.updateMetadataWithDecommissionStatus(DecommissionStatus.FAILED, statusUpdateListener());
            }
        });
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
        } else if (awarenessAttributes.contains(decommissionAttribute.attributeName()) == false) {
            msg = "invalid awareness attribute requested for decommissioning";
        } else if (forcedAwarenessAttributes.containsKey(decommissionAttribute.attributeName()) == false) {
            msg = "forced awareness attribute [" + forcedAwarenessAttributes.toString() + "] doesn't have the decommissioning attribute";
        }
        // we don't need to check for attributes presence in forced awareness attribute because, weights API ensures that weights are set
        // for all discovered routing attributes and forced attributes.
        // So, if the weight is not present for the attribute it could mean its a non routing node (eg. cluster manager)
        // And in that case, we are ok to proceed with the decommission. A routing node's attribute absence in forced awareness attribute is
        // a problem elsewhere

        if (msg != null) {
            throw new DecommissioningFailedException(decommissionAttribute, msg);
        }
    }

    private static void ensureToBeDecommissionedAttributeWeighedAway(ClusterState state, DecommissionAttribute decommissionAttribute) {
        WeightedRoutingMetadata weightedRoutingMetadata = state.metadata().weightedRoutingMetadata();
        if (weightedRoutingMetadata == null) {
            throw new DecommissioningFailedException(
                decommissionAttribute,
                "no weights are set to the attribute. Please set appropriate weights before triggering decommission action"
            );
        }
        WeightedRouting weightedRouting = weightedRoutingMetadata.getWeightedRouting();
        if (weightedRouting.attributeName().equals(decommissionAttribute.attributeName()) == false) {
            throw new DecommissioningFailedException(
                decommissionAttribute,
                "no weights are specified to attribute [" + decommissionAttribute.attributeName() + "]"
            );
        }
        // in case the weight is not set for the attribute value, then we know that attribute values was not part of discovered routing node
        // attribute or forced awareness attribute and in that case, we are ok if the attribute's value weight is not set. But if it's set,
        // its weight has to be zero
        Double attributeValueWeight = weightedRouting.weights().get(decommissionAttribute.attributeValue());
        if (attributeValueWeight != null && attributeValueWeight.equals(0.0) == false) {
            throw new DecommissioningFailedException(
                decommissionAttribute,
                "weight for decommissioned attribute is expected to be [0.0] but found [" + attributeValueWeight + "]"
            );
        }
    }

    private static void ensureEligibleRequest(
        DecommissionAttributeMetadata decommissionAttributeMetadata,
        DecommissionRequest decommissionRequest
    ) {
        String msg;
        DecommissionAttribute requestedDecommissionAttribute = decommissionRequest.getDecommissionAttribute();
        if (decommissionAttributeMetadata != null) {
            // check if the same attribute is registered and handle it accordingly
            if (decommissionAttributeMetadata.decommissionAttribute().equals(requestedDecommissionAttribute)) {
                switch (decommissionAttributeMetadata.status()) {
                    // for INIT - check if it is eligible internal retry
                    case INIT:
                        if (decommissionRequest.requestID().equals(decommissionAttributeMetadata.requestID()) == false) {
                            throw new DecommissioningFailedException(
                                requestedDecommissionAttribute,
                                "same request is already in status [INIT]"
                            );
                        }
                        break;
                    // for FAILED - we are good to process it again
                    case FAILED:
                        break;
                    case DRAINING:
                    case IN_PROGRESS:
                    case SUCCESSFUL:
                        msg = "same request is already in status [" + decommissionAttributeMetadata.status() + "]";
                        throw new DecommissioningFailedException(requestedDecommissionAttribute, msg);
                    default:
                        throw new IllegalStateException(
                            "unknown status [" + decommissionAttributeMetadata.status() + "] currently registered in metadata"
                        );
                }
            } else {
                switch (decommissionAttributeMetadata.status()) {
                    case SUCCESSFUL:
                        // one awareness attribute is already decommissioned. We will reject the new request
                        msg = "one awareness attribute ["
                            + decommissionAttributeMetadata.decommissionAttribute().toString()
                            + "] already successfully decommissioned, recommission before triggering another decommission";
                        throw new DecommissioningFailedException(requestedDecommissionAttribute, msg);
                    case DRAINING:
                    case IN_PROGRESS:
                    case INIT:
                        // it means the decommission has been initiated or is inflight. In that case, will fail new request
                        msg = "there's an inflight decommission request for attribute ["
                            + decommissionAttributeMetadata.decommissionAttribute().toString()
                            + "] is in progress, cannot process this request";
                        throw new DecommissioningFailedException(requestedDecommissionAttribute, msg);
                    case FAILED:
                        break;
                    default:
                        throw new IllegalStateException(
                            "unknown status [" + decommissionAttributeMetadata.status() + "] currently registered in metadata"
                        );
                }
            }
        }
    }

    private ActionListener<DecommissionStatus> statusUpdateListener() {
        return new ActionListener<>() {
            @Override
            public void onResponse(DecommissionStatus status) {
                logger.info("updated the decommission status to [{}]", status);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("unexpected failure occurred during decommission status update", e);
            }
        };
    }

    public void startRecommissionAction(final ActionListener<DeleteDecommissionStateResponse> listener) {
        /*
         * For abandoned requests, we might not really know if it actually restored the exclusion list.
         * And can land up in cases where even after recommission, exclusions are set(which is unexpected).
         * And by definition of OpenSearch - Clusters should have no voting configuration exclusions in normal operation.
         * Once the excluded nodes have stopped, clear the voting configuration exclusions with DELETE /_cluster/voting_config_exclusions.
         * And hence it is safe to remove the exclusion if any. User should make conscious choice before decommissioning awareness attribute.
         */
        clusterService.submitStateUpdateTask("delete-decommission-state", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                ClusterState newState = clearExclusionsAndGetState(currentState);
                logger.info("Deleting the decommission attribute from the cluster state");
                newState = deleteDecommissionAttributeInClusterState(newState);
                return newState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.error(() -> new ParameterizedMessage("failure during recommission action [{}]", source), e);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                logger.info("successfully cleared voting config exclusion and decommissioned attribute");
                assert newState.metadata().decommissionAttributeMetadata() == null;
                assert newState.coordinationMetadata().getVotingConfigExclusions().isEmpty();
                listener.onResponse(new DeleteDecommissionStateResponse(true));
            }
        });
    }
}

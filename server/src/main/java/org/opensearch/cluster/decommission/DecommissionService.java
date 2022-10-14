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
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateResponse;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.NotClusterManagerException;
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
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
 * <li>Remove to-be-decommissioned cluster-manager eligible nodes from voting config and wait for its abdication if it is active leader</li>
 * <li>Triggers weigh away for nodes having given awareness attribute to drain.</li>
 * <li>Once weighed away, the service triggers nodes decommission. This marks the decommission status as {@link DecommissionStatus#IN_PROGRESS}</li>
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
     * Once the status is updated, it tries to exclude to-be-decommissioned cluster manager eligible nodes from Voting Configuration
     *
     * @param decommissionAttribute register decommission attribute in the metadata request
     * @param listener register decommission listener
     */
    public void startDecommissionAction(
        final DecommissionAttribute decommissionAttribute,
        final ActionListener<DecommissionResponse> listener
    ) {
        // register the metadata with status as INIT as first step
        clusterService.submitStateUpdateTask("decommission [" + decommissionAttribute + "]", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                // validates if correct awareness attributes and forced awareness attribute set to the cluster before starting action
                validateAwarenessAttribute(decommissionAttribute, awarenessAttributes, forcedAwarenessAttributes);
                DecommissionAttributeMetadata decommissionAttributeMetadata = currentState.metadata().decommissionAttributeMetadata();
                // check that request is eligible to proceed
                ensureEligibleRequest(decommissionAttributeMetadata, decommissionAttribute);
                decommissionAttributeMetadata = new DecommissionAttributeMetadata(decommissionAttribute);
                logger.info("registering decommission metadata [{}] to execute action", decommissionAttributeMetadata.toString());
                return ClusterState.builder(currentState)
                    .metadata(Metadata.builder(currentState.metadata()).decommissionAttributeMetadata(decommissionAttributeMetadata))
                    .build();
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
                logger.info(
                    "registered decommission metadata for attribute [{}] with status [{}]",
                    decommissionAttributeMetadata.decommissionAttribute(),
                    decommissionAttributeMetadata.status()
                );
                decommissionClusterManagerNodes(decommissionAttributeMetadata.decommissionAttribute(), listener);
            }
        });
    }

    private synchronized void decommissionClusterManagerNodes(
        final DecommissionAttribute decommissionAttribute,
        ActionListener<DecommissionResponse> listener
    ) {
        ClusterState state = clusterService.getClusterApplierService().state();
        // since here metadata is already registered with INIT, we can guarantee that no new node with decommission attribute can further
        // join the cluster
        // and hence in further request lifecycle we are sure that no new to-be-decommission leader will join the cluster
        Set<DiscoveryNode> clusterManagerNodesToBeDecommissioned = filterNodesWithDecommissionAttribute(state, decommissionAttribute, true);
        logger.info(
            "resolved cluster manager eligible nodes [{}] that should be removed from Voting Configuration",
            clusterManagerNodesToBeDecommissioned.toString()
        );

        // remove all 'to-be-decommissioned' cluster manager eligible nodes from voting config
        Set<String> nodeIdsToBeExcluded = clusterManagerNodesToBeDecommissioned.stream()
            .map(DiscoveryNode::getId)
            .collect(Collectors.toSet());

        final Predicate<ClusterState> allNodesRemovedAndAbdicated = clusterState -> {
            final Set<String> votingConfigNodeIds = clusterState.getLastCommittedConfiguration().getNodeIds();
            return nodeIdsToBeExcluded.stream().noneMatch(votingConfigNodeIds::contains)
                && nodeIdsToBeExcluded.contains(clusterState.nodes().getClusterManagerNodeId()) == false
                && clusterState.nodes().getClusterManagerNodeId() != null;
        };

        ActionListener<Void> exclusionListener = new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                if (clusterService.getClusterApplierService().state().nodes().isLocalNodeElectedClusterManager()) {
                    if (nodeHasDecommissionedAttribute(clusterService.localNode(), decommissionAttribute)) {
                        // this is an unexpected state, as after exclusion of nodes having decommission attribute,
                        // this local node shouldn't have had the decommission attribute. Will send the failure response to the user
                        String errorMsg =
                            "unexpected state encountered [local node is to-be-decommissioned leader] while executing decommission request";
                        logger.error(errorMsg);
                        // will go ahead and clear the voting config and mark the status as false
                        clearVotingConfigExclusionAndUpdateStatus(false, false);
                        // we can send the failure response to the user here
                        listener.onFailure(new IllegalStateException(errorMsg));
                    } else {
                        logger.info("will attempt to fail decommissioned nodes as local node is eligible to process the request");
                        // we are good here to send the response now as the request is processed by an eligible active leader
                        // and to-be-decommissioned cluster manager is no more part of Voting Configuration and no more to-be-decommission
                        // nodes can be part of Voting Config
                        listener.onResponse(new DecommissionResponse(true));
                        failDecommissionedNodes(clusterService.getClusterApplierService().state());
                    }
                } else {
                    // explicitly calling listener.onFailure with NotClusterManagerException as the local node is not the cluster manager
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
            public void onFailure(Exception e) {
                listener.onFailure(e);
                // attempting to mark the status as FAILED
                decommissionController.updateMetadataWithDecommissionStatus(DecommissionStatus.FAILED, statusUpdateListener());
            }
        };

        if (allNodesRemovedAndAbdicated.test(state)) {
            exclusionListener.onResponse(null);
        } else {
            logger.debug("sending transport request to remove nodes [{}] from voting config", nodeIdsToBeExcluded.toString());
            // send a transport request to exclude to-be-decommissioned cluster manager eligible nodes from voting config
            decommissionController.excludeDecommissionedNodesFromVotingConfig(nodeIdsToBeExcluded, new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    logger.info(
                        "successfully removed decommissioned cluster manager eligible nodes [{}] from voting config ",
                        clusterManagerNodesToBeDecommissioned.toString()
                    );
                    final ClusterStateObserver abdicationObserver = new ClusterStateObserver(
                        clusterService,
                        TimeValue.timeValueSeconds(60L),
                        logger,
                        threadPool.getThreadContext()
                    );
                    final ClusterStateObserver.Listener abdicationListener = new ClusterStateObserver.Listener() {
                        @Override
                        public void onNewClusterState(ClusterState state) {
                            logger.debug("to-be-decommissioned node is no more the active leader");
                            exclusionListener.onResponse(null);
                        }

                        @Override
                        public void onClusterServiceClose() {
                            String errorMsg = "cluster service closed while waiting for abdication of to-be-decommissioned leader";
                            logger.warn(errorMsg);
                            listener.onFailure(new DecommissioningFailedException(decommissionAttribute, errorMsg));
                        }

                        @Override
                        public void onTimeout(TimeValue timeout) {
                            logger.info("timed out while waiting for abdication of to-be-decommissioned leader");
                            clearVotingConfigExclusionAndUpdateStatus(false, false);
                            listener.onFailure(
                                new OpenSearchTimeoutException(
                                    "timed out [{}] while waiting for abdication of to-be-decommissioned leader",
                                    timeout.toString()
                                )
                            );
                        }
                    };
                    // In case the cluster state is already processed even before this code is executed
                    // therefore testing first before attaching the listener
                    ClusterState currentState = clusterService.getClusterApplierService().state();
                    if (allNodesRemovedAndAbdicated.test(currentState)) {
                        abdicationListener.onNewClusterState(currentState);
                    } else {
                        logger.debug("waiting to abdicate to-be-decommissioned leader");
                        abdicationObserver.waitForNextChange(abdicationListener, allNodesRemovedAndAbdicated);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(
                        new ParameterizedMessage(
                            "failure in removing to-be-decommissioned cluster manager eligible nodes [{}] from voting config",
                            nodeIdsToBeExcluded.toString()
                        ),
                        e
                    );
                    exclusionListener.onFailure(e);
                }
            });
        }
    }

    private void failDecommissionedNodes(ClusterState state) {
        // this method ensures no matter what, we always exit from this function after clearing the voting config exclusion
        DecommissionAttributeMetadata decommissionAttributeMetadata = state.metadata().decommissionAttributeMetadata();
        DecommissionAttribute decommissionAttribute = decommissionAttributeMetadata.decommissionAttribute();
        decommissionController.updateMetadataWithDecommissionStatus(DecommissionStatus.IN_PROGRESS, new ActionListener<>() {
            @Override
            public void onResponse(DecommissionStatus status) {
                logger.info("updated the decommission status to [{}]", status);
                // execute nodes decommissioning
                decommissionController.removeDecommissionedNodes(
                    filterNodesWithDecommissionAttribute(clusterService.getClusterApplierService().state(), decommissionAttribute, false),
                    "nodes-decommissioned",
                    TimeValue.timeValueSeconds(120L),
                    new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void unused) {
                            clearVotingConfigExclusionAndUpdateStatus(true, true);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            clearVotingConfigExclusionAndUpdateStatus(false, false);
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
                clearVotingConfigExclusionAndUpdateStatus(false, false);
            }
        });
    }

    private void clearVotingConfigExclusionAndUpdateStatus(boolean decommissionSuccessful, boolean waitForRemoval) {
        decommissionController.clearVotingConfigExclusion(new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                logger.info(
                    "successfully cleared voting config exclusion after completing decommission action, proceeding to update metadata"
                );
                DecommissionStatus updateStatusWith = decommissionSuccessful ? DecommissionStatus.SUCCESSFUL : DecommissionStatus.FAILED;
                decommissionController.updateMetadataWithDecommissionStatus(updateStatusWith, statusUpdateListener());
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug(
                    new ParameterizedMessage("failure in clearing voting config exclusion after processing decommission request"),
                    e
                );
                decommissionController.updateMetadataWithDecommissionStatus(DecommissionStatus.FAILED, statusUpdateListener());
            }
        }, waitForRemoval);
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
        } else if (forcedAwarenessAttributes.get(decommissionAttribute.attributeName())
            .contains(decommissionAttribute.attributeValue()) == false) {
                msg = "invalid awareness attribute value requested for decommissioning. Set forced awareness values before to decommission";
            }

        if (msg != null) {
            throw new DecommissioningFailedException(decommissionAttribute, msg);
        }
    }

    private static void ensureEligibleRequest(
        DecommissionAttributeMetadata decommissionAttributeMetadata,
        DecommissionAttribute requestedDecommissionAttribute
    ) {
        String msg = null;
        if (decommissionAttributeMetadata != null) {
            // check if the same attribute is registered and handle it accordingly
            if (decommissionAttributeMetadata.decommissionAttribute().equals(requestedDecommissionAttribute)) {
                switch (decommissionAttributeMetadata.status()) {
                    // for INIT and FAILED - we are good to process it again
                    case INIT:
                    case FAILED:
                        break;
                    case IN_PROGRESS:
                    case SUCCESSFUL:
                        msg = "same request is already in status [" + decommissionAttributeMetadata.status() + "]";
                        break;
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
                        break;
                    case IN_PROGRESS:
                    case INIT:
                        // it means the decommission has been initiated or is inflight. In that case, will fail new request
                        msg = "there's an inflight decommission request for attribute ["
                            + decommissionAttributeMetadata.decommissionAttribute().toString()
                            + "] is in progress, cannot process this request";
                        break;
                    case FAILED:
                        break;
                    default:
                        throw new IllegalStateException(
                            "unknown status [" + decommissionAttributeMetadata.status() + "] currently registered in metadata"
                        );
                }
            }
        }

        if (msg != null) {
            throw new DecommissioningFailedException(requestedDecommissionAttribute, msg);
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
        decommissionController.clearVotingConfigExclusion(new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                logger.info("successfully cleared voting config exclusion for deleting the decommission.");
                deleteDecommissionState(listener);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failure in clearing voting config during delete_decommission request.", e);
                listener.onFailure(e);
            }
        }, false);
    }

    void deleteDecommissionState(ActionListener<DeleteDecommissionStateResponse> listener) {
        clusterService.submitStateUpdateTask("delete_decommission_state", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                logger.info("Deleting the decommission attribute from the cluster state");
                Metadata metadata = currentState.metadata();
                Metadata.Builder mdBuilder = Metadata.builder(metadata);
                mdBuilder.removeCustom(DecommissionAttributeMetadata.TYPE);
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.error(() -> new ParameterizedMessage("Failed to clear decommission attribute. [{}]", source), e);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                // Cluster state processed for deleting the decommission attribute.
                assert newState.metadata().decommissionAttributeMetadata() == null;
                listener.onResponse(new DeleteDecommissionStateResponse(true));
            }
        });
    }

    /**
     * Utility method to check if the node has decommissioned attribute
     *
     * @param discoveryNode node to check on
     * @param decommissionAttribute attribute to be checked with
     * @return true or false based on whether node has decommissioned attribute
     */
    public static boolean nodeHasDecommissionedAttribute(DiscoveryNode discoveryNode, DecommissionAttribute decommissionAttribute) {
        String nodeAttributeValue = discoveryNode.getAttributes().get(decommissionAttribute.attributeName());
        return nodeAttributeValue != null && nodeAttributeValue.equals(decommissionAttribute.attributeValue());
    }

    /**
     * Utility method to check if the node is commissioned or not
     *
     * @param discoveryNode node to check on
     * @param metadata metadata present current which will be used to check the commissioning status of the node
     * @return if the node is commissioned or not
     */
    public static boolean nodeCommissioned(DiscoveryNode discoveryNode, Metadata metadata) {
        DecommissionAttributeMetadata decommissionAttributeMetadata = metadata.decommissionAttributeMetadata();
        if (decommissionAttributeMetadata != null) {
            DecommissionAttribute decommissionAttribute = decommissionAttributeMetadata.decommissionAttribute();
            DecommissionStatus status = decommissionAttributeMetadata.status();
            if (decommissionAttribute != null && status != null) {
                if (nodeHasDecommissionedAttribute(discoveryNode, decommissionAttribute)
                    && (status.equals(DecommissionStatus.IN_PROGRESS) || status.equals(DecommissionStatus.SUCCESSFUL))) {
                    return false;
                }
            }
        }
        return true;
    }
}

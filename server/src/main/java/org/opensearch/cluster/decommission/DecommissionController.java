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
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.coordination.NodeRemovalClusterStateTaskExecutor;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.http.HttpStats;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.opensearch.action.admin.cluster.configuration.VotingConfigExclusionsHelper.clearExclusionsAndGetState;

/**
 * Helper controller class to remove list of nodes from the cluster and update status
 *
 * @opensearch.internal
 */

public class DecommissionController {

    private static final Logger logger = LogManager.getLogger(DecommissionController.class);

    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final ThreadPool threadPool;

    DecommissionController(
        ClusterService clusterService,
        TransportService transportService,
        AllocationService allocationService,
        ThreadPool threadPool
    ) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, logger);
        this.threadPool = threadPool;
    }

    /**
     * This method triggers batch of tasks for nodes to be decommissioned using executor {@link NodeRemovalClusterStateTaskExecutor}
     * Once the tasks are submitted, it waits for an expected cluster state to guarantee
     * that the expected decommissioned nodes are removed from the cluster
     *
     * @param nodesToBeDecommissioned set of the node to be decommissioned
     * @param reason reason of removal
     * @param timeout timeout for the request
     * @param nodesRemovedListener callback for the success or failure
     */
    public synchronized void removeDecommissionedNodes(
        Set<DiscoveryNode> nodesToBeDecommissioned,
        String reason,
        TimeValue timeout,
        ActionListener<Void> nodesRemovedListener
    ) {
        final Map<NodeRemovalClusterStateTaskExecutor.Task, ClusterStateTaskListener> nodesDecommissionTasks = new LinkedHashMap<>(
            nodesToBeDecommissioned.size()
        );
        nodesToBeDecommissioned.forEach(discoveryNode -> {
            final NodeRemovalClusterStateTaskExecutor.Task task = new NodeRemovalClusterStateTaskExecutor.Task(discoveryNode, reason);
            nodesDecommissionTasks.put(task, nodeRemovalExecutor);
        });

        logger.info("submitting state update task to remove [{}] nodes due to decommissioning", nodesToBeDecommissioned.toString());
        clusterService.submitStateUpdateTasks(
            "node-decommissioned",
            nodesDecommissionTasks,
            ClusterStateTaskConfig.build(Priority.URGENT),
            nodeRemovalExecutor
        );

        Predicate<ClusterState> allDecommissionedNodesRemovedPredicate = clusterState -> {
            Set<DiscoveryNode> intersection = Arrays.stream(clusterState.nodes().getNodes().values().toArray(new DiscoveryNode[0]))
                .collect(Collectors.toSet());
            intersection.retainAll(nodesToBeDecommissioned);
            return intersection.size() == 0;
        };

        final ClusterStateObserver observer = new ClusterStateObserver(clusterService, timeout, logger, threadPool.getThreadContext());

        final ClusterStateObserver.Listener removalListener = new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                logger.info("successfully removed all decommissioned nodes [{}] from the cluster", nodesToBeDecommissioned.toString());
                nodesRemovedListener.onResponse(null);
            }

            @Override
            public void onClusterServiceClose() {
                logger.warn(
                    "cluster service closed while waiting for removal of decommissioned nodes [{}]",
                    nodesToBeDecommissioned.toString()
                );
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                logger.info(
                    "timed out [{}] while waiting for removal of decommissioned nodes [{}]",
                    timeout.toString(),
                    nodesToBeDecommissioned.toString()
                );
                nodesRemovedListener.onFailure(
                    new OpenSearchTimeoutException(
                        "timed out [{}] while waiting for removal of decommissioned nodes [{}]",
                        timeout.toString(),
                        nodesToBeDecommissioned.toString()
                    )
                );
            }
        };

        if (allDecommissionedNodesRemovedPredicate.test(clusterService.getClusterApplierService().state())) {
            removalListener.onNewClusterState(clusterService.getClusterApplierService().state());
        } else {
            observer.waitForNextChange(removalListener, allDecommissionedNodesRemovedPredicate);
        }
    }

    /**
     * This method updates the status in the currently registered metadata.
     *
     * @param decommissionStatus status to update decommission metadata with
     * @param listener listener for response and failure
     */
    public void updateMetadataWithDecommissionStatus(DecommissionStatus decommissionStatus, ActionListener<DecommissionStatus> listener) {
        clusterService.submitStateUpdateTask("update-decommission-status", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                DecommissionAttributeMetadata decommissionAttributeMetadata = currentState.metadata().decommissionAttributeMetadata();
                assert decommissionAttributeMetadata != null && decommissionAttributeMetadata.decommissionAttribute() != null;
                logger.info(
                    "attempting to update current decommission status [{}] with expected status [{}]",
                    decommissionAttributeMetadata.status(),
                    decommissionStatus
                );
                // validateNewStatus can throw IllegalStateException if the sequence of update is not valid
                decommissionAttributeMetadata.validateNewStatus(decommissionStatus);
                decommissionAttributeMetadata = new DecommissionAttributeMetadata(
                    decommissionAttributeMetadata.decommissionAttribute(),
                    decommissionStatus,
                    decommissionAttributeMetadata.requestID()
                );
                ClusterState newState = ClusterState.builder(currentState)
                    .metadata(Metadata.builder(currentState.metadata()).decommissionAttributeMetadata(decommissionAttributeMetadata))
                    .build();

                // For terminal status we will go ahead and clear any exclusion that was added as part of decommission action
                if (decommissionStatus.equals(DecommissionStatus.SUCCESSFUL) || decommissionStatus.equals(DecommissionStatus.FAILED)) {
                    newState = clearExclusionsAndGetState(newState);
                }
                return newState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                DecommissionAttributeMetadata decommissionAttributeMetadata = newState.metadata().decommissionAttributeMetadata();
                assert decommissionAttributeMetadata != null;
                assert decommissionAttributeMetadata.status().equals(decommissionStatus);
                listener.onResponse(decommissionAttributeMetadata.status());
            }
        });
    }

    private void logActiveConnections(NodesStatsResponse nodesStatsResponse) {
        if (nodesStatsResponse == null || nodesStatsResponse.getNodes() == null) {
            logger.info("Node stats response received is null/empty.");
            return;
        }

        Map<String, Long> nodeActiveConnectionMap = new HashMap<>();
        List<NodeStats> responseNodes = nodesStatsResponse.getNodes();
        for (int i = 0; i < responseNodes.size(); i++) {
            HttpStats httpStats = responseNodes.get(i).getHttp();
            DiscoveryNode node = responseNodes.get(i).getNode();
            nodeActiveConnectionMap.put(node.getId(), httpStats.getServerOpen());
        }
        logger.info("Decommissioning node with connections : [{}]", nodeActiveConnectionMap);
    }

    void getActiveRequestCountOnDecommissionedNodes(Set<DiscoveryNode> decommissionedNodes) {
        if (decommissionedNodes == null || decommissionedNodes.isEmpty()) {
            return;
        }
        String[] nodes = decommissionedNodes.stream().map(DiscoveryNode::getId).toArray(String[]::new);
        if (nodes.length == 0) {
            return;
        }

        final NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(nodes);
        nodesStatsRequest.clear();
        nodesStatsRequest.addMetric(NodesStatsRequest.Metric.HTTP.metricName());

        transportService.sendRequest(
            transportService.getLocalNode(),
            NodesStatsAction.NAME,
            nodesStatsRequest,
            new TransportResponseHandler<NodesStatsResponse>() {
                @Override
                public void handleResponse(NodesStatsResponse response) {
                    logActiveConnections(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.error("Failure occurred while dumping connection for decommission nodes - ", exp.unwrapCause());
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public NodesStatsResponse read(StreamInput in) throws IOException {
                    return new NodesStatsResponse(in);
                }
            }
        );
    }
}

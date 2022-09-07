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
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsResponse;
import org.opensearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsRequest;
import org.opensearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsResponse;
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
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
     * Transport call to add nodes to voting config exclusion
     *
     * @param nodes set of nodes to be added to voting config exclusion list
     * @param listener callback for response or failure
     */
    public void excludeDecommissionedNodesFromVotingConfig(Set<String> nodes, ActionListener<Void> listener) {
        transportService.sendRequest(
            transportService.getLocalNode(),
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest(nodes.stream().toArray(String[]::new)),
            new TransportResponseHandler<AddVotingConfigExclusionsResponse>() {
                @Override
                public void handleResponse(AddVotingConfigExclusionsResponse response) {
                    listener.onResponse(null);
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
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

    /**
     * Transport call to clear voting config exclusion
     *
     * @param listener callback for response or failure
     */
    public void clearVotingConfigExclusion(ActionListener<Void> listener) {
        final ClearVotingConfigExclusionsRequest clearVotingConfigExclusionsRequest = new ClearVotingConfigExclusionsRequest();
        transportService.sendRequest(
            transportService.getLocalNode(),
            ClearVotingConfigExclusionsAction.NAME,
            clearVotingConfigExclusionsRequest,
            new TransportResponseHandler<ClearVotingConfigExclusionsResponse>() {
                @Override
                public void handleResponse(ClearVotingConfigExclusionsResponse response) {
                    listener.onResponse(null);
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
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
     * This method triggers batch of tasks for nodes to be decommissioned using executor {@link NodeRemovalClusterStateTaskExecutor}
     * Once the tasks are submitted, it waits for an expected cluster state to guarantee
     * that the expected decommissioned nodes are removed from the cluster
     *
     * @param nodesToBeDecommissioned set of the node to be decommissioned
     * @param reason reason of removal
     * @param timeout timeout for the request
     * @param nodesRemovedListener callback for the success or failure
     */
    public void removeDecommissionedNodes(
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
        clusterService.submitStateUpdateTasks(
            "node-decommissioned",
            nodesDecommissionTasks,
            ClusterStateTaskConfig.build(Priority.URGENT),
            nodeRemovalExecutor
        );

        Predicate<ClusterState> allDecommissionedNodesRemovedPredicate = clusterState -> {
            Set<DiscoveryNode> intersection = Arrays.stream(clusterState.nodes().getNodes().values().toArray(DiscoveryNode.class))
                .collect(Collectors.toSet());
            intersection.retainAll(nodesToBeDecommissioned);
            return intersection.size() == 0;
        };

        final ClusterStateObserver observer = new ClusterStateObserver(clusterService, timeout, logger, threadPool.getThreadContext());

        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                logger.info("successfully removed all decommissioned nodes [{}] from the cluster", nodesToBeDecommissioned.toString());
                nodesRemovedListener.onResponse(null);
            }

            @Override
            public void onClusterServiceClose() {
                logger.warn("cluster service closed while waiting for removal of decommissioned nodes.");
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                logger.info("timed out while waiting for removal of decommissioned nodes");
                nodesRemovedListener.onFailure(
                    new OpenSearchTimeoutException(
                        "timed out [{}] while waiting for removal of decommissioned nodes [{}] to take effect",
                        timeout.toString(),
                        nodesToBeDecommissioned.toString()
                    )
                );
            }
        }, allDecommissionedNodesRemovedPredicate);
    }

    /**
     * This method updates the status in the currently registered metadata.
     * This method also validates the status with its previous state before executing the request
     *
     * @param decommissionStatus status to update decommission metadata with
     * @param listener listener for response and failure
     */
    public void updateMetadataWithDecommissionStatus(DecommissionStatus decommissionStatus, ActionListener<DecommissionStatus> listener) {
        clusterService.submitStateUpdateTask(decommissionStatus.status(), new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                Metadata metadata = currentState.metadata();
                DecommissionAttributeMetadata decommissionAttributeMetadata = metadata.custom(DecommissionAttributeMetadata.TYPE);
                assert decommissionAttributeMetadata != null && decommissionAttributeMetadata.decommissionAttribute() != null;
                // we need to update the status only when the previous stage is just behind than expected stage
                // if the previous stage is already ahead of expected stage, we don't need to update the stage
                // For failures, we update it no matter what
                int previousStage = decommissionAttributeMetadata.status().stage();
                int expectedStage = decommissionStatus.stage();
                if (previousStage >= expectedStage) return currentState;
                if (expectedStage - previousStage != 1 && !decommissionStatus.equals(DecommissionStatus.FAILED)) {
                    throw new DecommissioningFailedException(
                        decommissionAttributeMetadata.decommissionAttribute(),
                        "invalid previous decommission status found while updating status"
                    );
                }
                Metadata.Builder mdBuilder = Metadata.builder(metadata);
                DecommissionAttributeMetadata newMetadata = decommissionAttributeMetadata.withUpdatedStatus(decommissionStatus);
                mdBuilder.putCustom(DecommissionAttributeMetadata.TYPE, newMetadata);
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                DecommissionAttributeMetadata decommissionAttributeMetadata = newState.metadata()
                    .custom(DecommissionAttributeMetadata.TYPE);
                logger.info("updated decommission status to [{}]", decommissionAttributeMetadata.status());
                listener.onResponse(decommissionAttributeMetadata.status());
            }
        });
    }
}

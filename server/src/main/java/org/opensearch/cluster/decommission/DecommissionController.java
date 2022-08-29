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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.coordination.NodeRemovalClusterStateTaskExecutor;
import org.opensearch.cluster.metadata.DecommissionAttributeMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Helper executor class to remove list of nodes from the cluster
 *
 * @opensearch.internal
 */

public class DecommissionController {

    private static final Logger logger = LogManager.getLogger(DecommissionController.class);

    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    DecommissionController(
        ClusterService clusterService,
        AllocationService allocationService,
        ThreadPool threadPool
    ) {
        this.clusterService = clusterService;
        this.nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, logger);
        this.threadPool = threadPool;
    }

    public void handleNodesDecommissionRequest(
        Set<DiscoveryNode> nodesToBeDecommissioned,
        String reason,
        TimeValue timeout,
        ActionListener<Void> nodesRemovedListener
    ) {
        final Map<NodeRemovalClusterStateTaskExecutor.Task, ClusterStateTaskListener> nodesDecommissionTasks = new LinkedHashMap<>();
        nodesToBeDecommissioned.forEach(discoveryNode -> {
            final NodeRemovalClusterStateTaskExecutor.Task task = new NodeRemovalClusterStateTaskExecutor.Task(discoveryNode, reason);
            nodesDecommissionTasks.put(task, nodeRemovalExecutor);
        });
        clusterService.submitStateUpdateTasks(
            "node-decommissioned",
            nodesDecommissionTasks,
            ClusterStateTaskConfig.build(Priority.IMMEDIATE),
            nodeRemovalExecutor
        );

        Predicate<ClusterState> allDecommissionedNodesRemovedPredicate = clusterState -> {
            Iterator<DiscoveryNode> nodesIter = clusterState.nodes().getNodes().valuesIt();
            while (nodesIter.hasNext()) {
                final DiscoveryNode node = nodesIter.next();
                // check if the node is part of node decommissioned list
                if (nodesToBeDecommissioned.contains(node)) {
                    return false;
                }
            }
            return true;
        };

        final ClusterStateObserver observer = new ClusterStateObserver(
            clusterService,
            timeout,
            logger,
            threadPool.getThreadContext()
        );

        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                logger.info("successfully removed all decommissioned nodes [{}] from the cluster", nodesToBeDecommissioned.toString());
                nodesRemovedListener.onResponse(null);
            }

            @Override
            public void onClusterServiceClose() {
                logger.debug("cluster service closed while waiting for removal of decommissioned nodes.");
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                logger.info("timed out while waiting for removal of decommissioned nodes");
                nodesRemovedListener.onFailure(
                    new OpenSearchTimeoutException(
                        "timed out waiting for removal of decommissioned nodes [{}] to take effect",
                        nodesToBeDecommissioned.toString()
                    )
                );
            }
        }, allDecommissionedNodesRemovedPredicate);
    }

    public void updateMetadataWithDecommissionStatus(
        DecommissionStatus decommissionStatus,
        ActionListener<Void> listener
    ) {
        clusterService.submitStateUpdateTask(decommissionStatus.status(), new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                Metadata metadata = currentState.metadata();
                DecommissionAttributeMetadata decommissionAttributeMetadata = metadata.custom(DecommissionAttributeMetadata.TYPE);
                assert decommissionAttributeMetadata != null && decommissionAttributeMetadata.decommissionAttribute() != null;
                assert assertIncrementalStatusOrFailed(decommissionAttributeMetadata.status(), decommissionStatus);
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
                DecommissionAttributeMetadata decommissionAttributeMetadata = newState.metadata().custom(DecommissionAttributeMetadata.TYPE);
                assert decommissionAttributeMetadata.status().equals(decommissionStatus);
                listener.onResponse(null);
            }
        });
    }

    private static boolean assertIncrementalStatusOrFailed(DecommissionStatus oldStatus, DecommissionStatus newStatus) {
        if (newStatus.equals(DecommissionStatus.DECOMMISSION_FAILED)) return true;
        else if (newStatus.equals(DecommissionStatus.DECOMMISSION_SUCCESSFUL)) {
            return oldStatus.equals(DecommissionStatus.DECOMMISSION_IN_PROGRESS);
        } else if (newStatus.equals(DecommissionStatus.DECOMMISSION_IN_PROGRESS)) {
            return oldStatus.equals(DecommissionStatus.DECOMMISSION_INIT);
        }
        return true;
    }
}

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
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.coordination.NodeRemovalClusterStateTaskExecutor;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Helper executor class to remove list of nodes from the cluster
 *
 * @opensearch.internal
 */

public class DecommissionHelper {

    private static final Logger logger = LogManager.getLogger(DecommissionHelper.class);

    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    DecommissionHelper(
        ClusterService clusterService,
        AllocationService allocationService,
        ThreadPool threadPool
    ) {
        this.clusterService = clusterService;
        this.nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, logger);
        this.threadPool = threadPool;
    }

    public void handleNodesDecommissionRequest(
        List<DiscoveryNode> nodesToBeDecommissioned,
        String reason,
        TimeValue timeout,
        Predicate<ClusterState> allDecommissionedNodesRemoved,
        ActionListener<ClusterStateUpdateResponse> nodesRemovedListener
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
                nodesRemovedListener.onResponse(new ClusterStateUpdateResponse(true));
            }

            @Override
            public void onClusterServiceClose() {
                logger.debug("cluster service closed while waiting for removal of decommissioned nodes.");
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                logger.info("timed out while waiting for removal of decommissioned nodes");
                nodesRemovedListener.onResponse(new ClusterStateUpdateResponse(false));
            }
        }, allDecommissionedNodesRemoved);
    }
}

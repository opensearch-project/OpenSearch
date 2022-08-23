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
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.coordination.NodeRemovalClusterStateTaskExecutor;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper executor class to remove list of nodes from the cluster
 *
 * @opensearch.internal
 */

public class DecommissionHelper {

    private static final Logger logger = LogManager.getLogger(DecommissionHelper.class);

    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;
    private final ClusterService clusterService;

    DecommissionHelper(ClusterService clusterService, AllocationService allocationService) {
        this.clusterService = clusterService;
        this.nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, logger);
    }

    public void handleNodesDecommissionRequest(List<DiscoveryNode> nodesToBeDecommissioned, String reason) {
        final Map<NodeRemovalClusterStateTaskExecutor.Task, ClusterStateTaskListener> nodesDecommissionTasks = new LinkedHashMap<>();
        nodesToBeDecommissioned.forEach(discoveryNode -> {
            final NodeRemovalClusterStateTaskExecutor.Task task = new NodeRemovalClusterStateTaskExecutor.Task(discoveryNode, reason);
            nodesDecommissionTasks.put(task, nodeRemovalExecutor);
        });
        final String source = "node-decommissioned";
        clusterService.submitStateUpdateTasks(
            source,
            nodesDecommissionTasks,
            ClusterStateTaskConfig.build(Priority.IMMEDIATE),
            nodeRemovalExecutor
        );
    }
}

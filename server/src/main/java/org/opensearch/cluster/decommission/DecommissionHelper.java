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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.coordination.NodeRemovalClusterStateTaskExecutor;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterManagerService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DecommissionHelper {

    private static final Logger logger = LogManager.getLogger(DecommissionHelper.class);

    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;
    private final ClusterManagerService clusterManagerService;

    DecommissionHelper(
        ClusterManagerService clusterManagerService,
        NodeRemovalClusterStateTaskExecutor nodeRemovalClusterStateTaskExecutor
    ) {
        this.nodeRemovalExecutor = nodeRemovalClusterStateTaskExecutor;
        this.clusterManagerService = clusterManagerService;
    }

    private void handleNodesDecommissionRequest(List<DiscoveryNode> nodesToBeDecommissioned, String reason) {
        final Map<NodeRemovalClusterStateTaskExecutor.Task, ClusterStateTaskListener> nodesDecommissionTasks = new LinkedHashMap<>();
        nodesToBeDecommissioned.forEach(discoveryNode -> {
            final NodeRemovalClusterStateTaskExecutor.Task task = new NodeRemovalClusterStateTaskExecutor.Task(
                discoveryNode, reason
            );
            nodesDecommissionTasks.put(task, nodeRemovalExecutor);
        });
        final String source = "node-decommissioned";
        clusterManagerService.submitStateUpdateTasks(
            source,
            nodesDecommissionTasks,
            ClusterStateTaskConfig.build(Priority.IMMEDIATE),
            nodeRemovalExecutor
        );
    }
}

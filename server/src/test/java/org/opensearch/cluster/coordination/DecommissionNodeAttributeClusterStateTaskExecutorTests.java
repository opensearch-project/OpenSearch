/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DecommissionNodeAttributeClusterStateTaskExecutorTests extends OpenSearchTestCase {

    public void testRemoveNodesForDecommissionedAttribute() throws Exception{
        final AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.disassociateDeadNodes(any(ClusterState.class), eq(true), any(String.class))).thenAnswer(
            im -> im.getArguments()[0]
        );
        final AtomicReference<ClusterState> remainingNodesClusterState = new AtomicReference<>();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).build();

        logger.info("--> adding five nodes on same zone_1");
        clusterState = addNodes(clusterState, allocationService, "zone_1", "node1", "node2", "node3", "node4", "node5");

        logger.info("--> adding five nodes on same zone_2");
        clusterState = addNodes(clusterState, allocationService, "zone_2", "node6", "node7", "node8", "node9", "node10");

        logger.info("--> adding five nodes on same zone_3");
        clusterState = addNodes(clusterState, allocationService, "zone_3", "node11", "node12", "node13", "node14", "node15");

        final DecommissionNodeAttributeClusterStateTaskExecutor executor = new DecommissionNodeAttributeClusterStateTaskExecutor(allocationService, logger) {
            @Override
            protected ClusterState remainingNodesClusterState(ClusterState currentState, DiscoveryNodes.Builder remainingNodesBuilder) {
                remainingNodesClusterState.set(super.remainingNodesClusterState(currentState, remainingNodesBuilder));
                return remainingNodesClusterState.get();
            }
        };

        final List<DecommissionNodeAttributeClusterStateTaskExecutor.Task> tasks = new ArrayList<>();
        tasks.add(new DecommissionNodeAttributeClusterStateTaskExecutor.Task(
            "zone",
            "zone_3",
            "decommissioned")
        );

        final ClusterStateTaskExecutor.ClusterTasksResult<DecommissionNodeAttributeClusterStateTaskExecutor.Task> result = executor.execute(
            clusterState,
            tasks
        );
        verify(allocationService).disassociateDeadNodes(eq(remainingNodesClusterState.get()), eq(true), any(String.class));
    }

    private ClusterState addNodes(ClusterState clusterState, AllocationService allocationService, String zone, String... nodeIds) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.nodes());
        org.opensearch.common.collect.List.of(nodeIds).forEach(nodeId -> nodeBuilder.add(newNode(nodeId, singletonMap("zone", zone))));
        clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();
        return allocationService.reroute(clusterState, "reroute");
    }

    private DiscoveryNode newNode(String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(
            nodeId,
            buildNewFakeTransportAddress(),
            attributes,
            CLUSTER_MANAGER_DATA_ROLES,
            Version.CURRENT
        );
    }

    private static Set<DiscoveryNodeRole> CLUSTER_MANAGER_DATA_ROLES = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE))
    );
}

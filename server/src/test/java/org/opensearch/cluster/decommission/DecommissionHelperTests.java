/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;

public class DecommissionHelperTests extends OpenSearchTestCase {

    private static ThreadPool threadPool;
    private static ClusterService clusterService;

    @BeforeClass
    public static void createThreadPoolAndClusterService() {
        threadPool = new TestThreadPool("test", Settings.EMPTY);
        clusterService = createClusterService(threadPool);
    }

    @AfterClass
    public static void shutdownThreadPoolAndClusterService() {
        clusterService.stop();
        threadPool.shutdown();
    }

//    public void testRemoveNodesForDecommissionRequest() {
//        final AllocationService allocationService = mock(AllocationService.class);
//        final ClusterService clusterService = mock(ClusterService.class);
//
//        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).build();
//
//        logger.info("--> adding five nodes on same zone_1");
//        clusterState = addNodes(clusterState, "zone_1", "node1", "node2", "node3", "node4", "node5");
//
//        logger.info("--> adding five nodes on same zone_2");
//        clusterState = addNodes(clusterState, "zone_2", "node6", "node7", "node8", "node9", "node10");
//
//        logger.info("--> adding five nodes on same zone_3");
//        clusterState = addNodes(clusterState, "zone_3", "node11", "node12", "node13", "node14", "node15");
//
//        when(clusterService.state()).thenReturn(clusterState);
//
//        final DecommissionHelper decommissionHelper = new DecommissionHelper(clusterService, allocationService);
//
//        List<DiscoveryNode> nodesToBeRemoved = new ArrayList<>();
//        nodesToBeRemoved.add(clusterState.nodes().get("node11"));
//        nodesToBeRemoved.add(clusterState.nodes().get("node12"));
//        nodesToBeRemoved.add(clusterState.nodes().get("node13"));
//        nodesToBeRemoved.add(clusterState.nodes().get("node14"));
//        nodesToBeRemoved.add(clusterState.nodes().get("node15"));
//
//        decommissionHelper.handleNodesDecommissionRequest(nodesToBeRemoved, "unit-test");
//        assertEquals((clusterService.state().nodes().getSize()), 10);
//    }

    private ClusterState addNodes(ClusterState clusterState, String zone, String... nodeIds) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.nodes());
        org.opensearch.common.collect.List.of(nodeIds).forEach(nodeId -> nodeBuilder.add(newNode(nodeId, singletonMap("zone", zone))));
        clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();
        return clusterState;
    }

    private static DiscoveryNode newNode(String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), attributes, CLUSTER_MANAGER_DATA_ROLES, Version.CURRENT);
    }

    final private static Set<DiscoveryNodeRole> CLUSTER_MANAGER_DATA_ROLES = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE))
    );
}

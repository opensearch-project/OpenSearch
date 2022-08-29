/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsActionTests;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.opensearch.cluster.ClusterState.builder;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;

public class DecommissionControllerTests extends OpenSearchTestCase {

    private static ThreadPool threadPool;
    private static ClusterService clusterService;
    private DecommissionController decommissionController;
    private ClusterStateObserver clusterStateObserver;

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

    @Before
    public void setupForTests() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).build();
        logger.info("--> adding five nodes on same zone_1");
        clusterState = addNodes(clusterState, "zone_1", "node1", "node2", "node3", "node4", "node5");
        logger.info("--> adding five nodes on same zone_2");
        clusterState = addNodes(clusterState, "zone_2", "node6", "node7", "node8", "node9", "node10");
        logger.info("--> adding five nodes on same zone_3");
        clusterState = addNodes(clusterState, "zone_3", "node11", "node12", "node13", "node14", "node15");

        final ClusterState.Builder builder = builder(clusterState);
        setState(clusterService, builder);

        final AllocationService allocationService = mock(AllocationService.class);
        decommissionController = new DecommissionController(clusterService, allocationService, threadPool);
        clusterStateObserver = new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext());
    }

    public void testRemoveNodesForDecommissionRequest() throws InterruptedException{
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        Set<DiscoveryNode> nodesToBeRemoved = new HashSet<>();
        nodesToBeRemoved.add(clusterService.state().nodes().get("node11"));
        nodesToBeRemoved.add(clusterService.state().nodes().get("node12"));
        nodesToBeRemoved.add(clusterService.state().nodes().get("node13"));
        nodesToBeRemoved.add(clusterService.state().nodes().get("node14"));
        nodesToBeRemoved.add(clusterService.state().nodes().get("node15"));

        ActionListener<ClusterStateUpdateResponse> actionListener = new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                logger.info("test");
            }

            @Override
            public void onFailure(Exception e) {
            }
        };

        clusterStateObserver.waitForNextChange(new UpdateClusterStateForDecommission(countDownLatch, nodesToBeRemoved));
        decommissionController.handleNodesDecommissionRequest(
            nodesToBeRemoved,
            "unit-test",
            TimeValue.timeValueSeconds(30L),
            actionListener
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertEquals(clusterService.getClusterApplierService().state().nodes().getDataNodes().size(), 10);
    }

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

    private static class UpdateClusterStateForDecommission implements ClusterStateObserver.Listener {

        final CountDownLatch doneLatch;
        final Set<DiscoveryNode> discoveryNodes;

        UpdateClusterStateForDecommission(CountDownLatch latch, Set<DiscoveryNode> discoveryNodes) {
            this.doneLatch = latch;
            this.discoveryNodes = discoveryNodes;
        }

        @Override
        public void onNewClusterState(ClusterState state) {
            clusterService.getClusterManagerService().submitStateUpdateTask("decommission", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    assertThat(currentState, sameInstance(state));
                    final DiscoveryNodes.Builder remainingNodesBuilder = DiscoveryNodes.builder(currentState.nodes());
                    for (DiscoveryNode nodeToBeRemoved : discoveryNodes) {
                        remainingNodesBuilder.remove(nodeToBeRemoved);
                    }
                    return ClusterState.builder(currentState).nodes(remainingNodesBuilder).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    throw new AssertionError("unexpected failure", e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    doneLatch.countDown();
                }
            });
        }

        @Override
        public void onClusterServiceClose() {
            throw new AssertionError("unexpected close");
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            throw new AssertionError("unexpected timeout");
        }
    }
}

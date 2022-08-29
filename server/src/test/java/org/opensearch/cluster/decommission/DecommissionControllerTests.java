/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsActionTests;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.metadata.DecommissionAttributeMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.ImmutableOpenMap;
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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.cluster.ClusterState.builder;
import static org.opensearch.cluster.OpenSearchAllocationTestCase.createAllocationService;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;

public class DecommissionControllerTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private AllocationService allocationService;
    private DecommissionController decommissionController;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("test", Settings.EMPTY);
        clusterService = createClusterService(threadPool);
        allocationService = createAllocationService();
    }

    @Before
    public void setDefaultClusterState() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).build();
        logger.info("--> adding five nodes on same zone_1");
        clusterState = addNodes(clusterState, "zone_1", "node1", "node2", "node3", "node4", "node5");
        logger.info("--> adding five nodes on same zone_2");
        clusterState = addNodes(clusterState, "zone_2", "node6", "node7", "node8", "node9", "node10");
        logger.info("--> adding five nodes on same zone_3");
        clusterState = addNodes(clusterState, "zone_3", "node11", "node12", "node13", "node14", "node15");
        clusterState = setLocalNodeAsClusterManagerNode(clusterState, "node1");
        final ClusterState.Builder builder = builder(clusterState);
        setState(
            clusterService,
            builder
        );
        decommissionController = new DecommissionController(clusterService, allocationService, threadPool);
    }

    @After
    public void shutdownThreadPoolAndClusterService() {
        clusterService.stop();
        threadPool.shutdown();
    }

    public void testNodesRemovedForDecommissionRequestSuccessfulResponse() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Set<DiscoveryNode> nodesToBeRemoved = new HashSet<>();
        nodesToBeRemoved.add(clusterService.state().nodes().get("node11"));
        nodesToBeRemoved.add(clusterService.state().nodes().get("node12"));
        nodesToBeRemoved.add(clusterService.state().nodes().get("node13"));
        nodesToBeRemoved.add(clusterService.state().nodes().get("node14"));
        nodesToBeRemoved.add(clusterService.state().nodes().get("node15"));

        decommissionController.handleNodesDecommissionRequest(
            nodesToBeRemoved,
            "unit-test",
            TimeValue.timeValueSeconds(30L),
            new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    countDownLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail("there shouldn't have been any failure");
                }
            }
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        // test all 5 nodes removed and cluster has 10 nodes
        Set<DiscoveryNode> nodes = StreamSupport.stream(
            clusterService.getClusterApplierService().state().nodes().spliterator(), false
        ).collect(Collectors.toSet());
        assertEquals(nodes.size(), 10);
        // test no nodes part of zone-3
        for (DiscoveryNode node : nodes) {
            assertNotEquals(node.getAttributes().get("zone"), "zone-1");
        }
    }

    public void testTimesOut() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Set<DiscoveryNode> nodesToBeRemoved = new HashSet<>();
        nodesToBeRemoved.add(clusterService.state().nodes().get("node11"));
        nodesToBeRemoved.add(clusterService.state().nodes().get("node12"));
        nodesToBeRemoved.add(clusterService.state().nodes().get("node13"));
        nodesToBeRemoved.add(clusterService.state().nodes().get("node14"));
        nodesToBeRemoved.add(clusterService.state().nodes().get("node15"));
        decommissionController.handleNodesDecommissionRequest(
            nodesToBeRemoved,
            "unit-test",
            TimeValue.timeValueMillis(2),
            new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    fail("response shouldn't have been called");
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e, instanceOf(OpenSearchTimeoutException.class));
                    assertThat(e.getMessage(), startsWith("timed out waiting for removal of decommissioned nodes"));
                    countDownLatch.countDown();
                }
            }
        );
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
    }

    public void testSuccessfulDecommissionStatusMetadataUpdate() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        DecommissionAttributeMetadata oldMetadata = new DecommissionAttributeMetadata(
            new DecommissionAttribute("zone", "zone-1"),
            DecommissionStatus.DECOMMISSION_IN_PROGRESS
        );
        ClusterState state = clusterService.state();
        Metadata metadata = state.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(metadata);
        mdBuilder.putCustom(DecommissionAttributeMetadata.TYPE, oldMetadata);
        state = ClusterState.builder(state).metadata(mdBuilder).build();
        setState(clusterService, state);

        decommissionController.updateMetadataWithDecommissionStatus(
            DecommissionStatus.DECOMMISSION_SUCCESSFUL,
            new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    countDownLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail("decommission status update failed");
                }
            }
        );
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        ClusterState newState = clusterService.getClusterApplierService().state();
        DecommissionAttributeMetadata decommissionAttributeMetadata = newState.metadata().custom(DecommissionAttributeMetadata.TYPE);
        assertEquals(decommissionAttributeMetadata.status(), DecommissionStatus.DECOMMISSION_SUCCESSFUL);
    }

    private ClusterState addNodes(ClusterState clusterState, String zone, String... nodeIds) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.nodes());
        org.opensearch.common.collect.List.of(nodeIds).forEach(nodeId -> nodeBuilder.add(newNode(nodeId, singletonMap("zone", zone))));
        clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();
        return clusterState;
    }

    private ClusterState setLocalNodeAsClusterManagerNode(ClusterState clusterState, String nodeId) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.nodes());
        nodeBuilder.localNodeId(nodeId);
        nodeBuilder.clusterManagerNodeId(nodeId);
        clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();
        return clusterState;
    }

    private static DiscoveryNode newNode(String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), attributes, CLUSTER_MANAGER_DATA_ROLE, Version.CURRENT);
    }

    final private static Set<DiscoveryNodeRole> CLUSTER_MANAGER_DATA_ROLE = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE))
    );
}

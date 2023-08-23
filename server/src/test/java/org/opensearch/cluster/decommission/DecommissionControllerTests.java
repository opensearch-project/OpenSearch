/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.TransportClearVotingConfigExclusionsAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.opensearch.cluster.ClusterState.builder;
import static org.opensearch.cluster.OpenSearchAllocationTestCase.createAllocationService;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class DecommissionControllerTests extends OpenSearchTestCase {

    private static ThreadPool threadPool;
    private static ClusterService clusterService;
    private TransportService transportService;
    private AllocationService allocationService;
    private DecommissionController decommissionController;
    private ClusterSettings clusterSettings;

    @Before
    public void setTransportServiceAndDefaultClusterState() {
        threadPool = new TestThreadPool("test", Settings.EMPTY);
        allocationService = createAllocationService();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).build();
        logger.info("--> adding five nodes on same zone_1");
        clusterState = addNodes(clusterState, "zone_1", "node1", "node2", "node3", "node4", "node5");
        logger.info("--> adding five nodes on same zone_2");
        clusterState = addNodes(clusterState, "zone_2", "node6", "node7", "node8", "node9", "node10");
        logger.info("--> adding five nodes on same zone_3");
        clusterState = addNodes(clusterState, "zone_3", "node11", "node12", "node13", "node14", "node15");
        clusterState = setLocalNodeAsClusterManagerNode(clusterState, "node1");
        clusterState = setThreeNodesInVotingConfig(clusterState);
        final ClusterState.Builder builder = builder(clusterState);
        clusterService = createClusterService(threadPool, clusterState.nodes().get("node1"));
        setState(clusterService, builder);
        final MockTransport transport = new MockTransport();
        transportService = transport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> clusterService.state().nodes().get("node1"),
            null,
            emptySet()
        );

        final Settings.Builder nodeSettingsBuilder = Settings.builder();
        final Settings nodeSettings = nodeSettingsBuilder.build();
        clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        new TransportAddVotingConfigExclusionsAction(
            nodeSettings,
            clusterSettings,
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY))
        ); // registers action

        new TransportClearVotingConfigExclusionsAction(
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY))
        ); // registers action

        transportService.start();
        transportService.acceptIncomingRequests();
        decommissionController = new DecommissionController(clusterService, transportService, allocationService, threadPool);
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

        decommissionController.removeDecommissionedNodes(
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
        Set<DiscoveryNode> nodes = StreamSupport.stream(clusterService.getClusterApplierService().state().nodes().spliterator(), false)
            .collect(Collectors.toSet());
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
        final AtomicReference<Exception> exceptionReference = new AtomicReference<>();
        decommissionController.removeDecommissionedNodes(
            nodesToBeRemoved,
            "unit-test-timeout",
            TimeValue.timeValueMillis(0),
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    countDownLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    exceptionReference.set(e);
                    countDownLatch.countDown();
                }
            }
        );
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        MatcherAssert.assertThat("Expected onFailure to be called", exceptionReference.get(), notNullValue());
        MatcherAssert.assertThat(exceptionReference.get(), instanceOf(OpenSearchTimeoutException.class));
        MatcherAssert.assertThat(exceptionReference.get().getMessage(), containsString("waiting for removal of decommissioned nodes"));
    }

    public void testSuccessfulDecommissionStatusMetadataUpdate() throws InterruptedException {
        Map<DecommissionStatus, Set<DecommissionStatus>> decommissionStateTransitionMap = Map.of(
            DecommissionStatus.INIT,
            Set.of(DecommissionStatus.DRAINING, DecommissionStatus.IN_PROGRESS),
            DecommissionStatus.DRAINING,
            Set.of(DecommissionStatus.IN_PROGRESS),
            DecommissionStatus.IN_PROGRESS,
            Set.of(DecommissionStatus.SUCCESSFUL)
        );

        for (Map.Entry<DecommissionStatus, Set<DecommissionStatus>> entry : decommissionStateTransitionMap.entrySet()) {
            for (DecommissionStatus val : entry.getValue()) {
                verifyDecommissionStatusTransition(entry.getKey(), val);
            }
        }
    }

    public void testSuccessfulDecommissionStatusMetadataUpdateForFailedState() throws InterruptedException {
        Map<DecommissionStatus, Set<DecommissionStatus>> decommissionStateTransitionMap = Map.of(
            DecommissionStatus.INIT,
            Set.of(DecommissionStatus.FAILED),
            DecommissionStatus.DRAINING,
            Set.of(DecommissionStatus.FAILED),
            DecommissionStatus.IN_PROGRESS,
            Set.of(DecommissionStatus.FAILED)
        );

        for (Map.Entry<DecommissionStatus, Set<DecommissionStatus>> entry : decommissionStateTransitionMap.entrySet()) {
            for (DecommissionStatus val : entry.getValue()) {
                verifyDecommissionStatusTransition(entry.getKey(), val);
            }
        }
    }

    private void verifyDecommissionStatusTransition(DecommissionStatus currentStatus, DecommissionStatus newStatus)
        throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        DecommissionAttributeMetadata oldMetadata = new DecommissionAttributeMetadata(
            new DecommissionAttribute("zone", "zone-1"),
            currentStatus,
            randomAlphaOfLength(10)
        );
        ClusterState state = clusterService.state();
        Metadata metadata = state.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(metadata);
        mdBuilder.decommissionAttributeMetadata(oldMetadata);
        state = ClusterState.builder(state).metadata(mdBuilder).build();
        setState(clusterService, state);

        decommissionController.updateMetadataWithDecommissionStatus(newStatus, new ActionListener<DecommissionStatus>() {
            @Override
            public void onResponse(DecommissionStatus status) {
                assertEquals(newStatus, status);
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("decommission status update failed");
                countDownLatch.countDown();
            }
        });
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        ClusterState newState = clusterService.getClusterApplierService().state();
        DecommissionAttributeMetadata decommissionAttributeMetadata = newState.metadata().decommissionAttributeMetadata();
        assertEquals(decommissionAttributeMetadata.status(), newStatus);
    }

    private ClusterState addNodes(ClusterState clusterState, String zone, String... nodeIds) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.nodes());
        List.of(nodeIds).forEach(nodeId -> nodeBuilder.add(newNode(nodeId, singletonMap("zone", zone))));
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

    private ClusterState setThreeNodesInVotingConfig(ClusterState clusterState) {
        final CoordinationMetadata.VotingConfiguration votingConfiguration = CoordinationMetadata.VotingConfiguration.of(
            clusterState.nodes().get("node1"),
            clusterState.nodes().get("node6"),
            clusterState.nodes().get("node11")
        );

        Metadata.Builder builder = Metadata.builder()
            .coordinationMetadata(
                CoordinationMetadata.builder()
                    .lastAcceptedConfiguration(votingConfiguration)
                    .lastCommittedConfiguration(votingConfiguration)
                    .build()
            );
        clusterState = ClusterState.builder(clusterState).metadata(builder).build();
        return clusterState;
    }

    private static DiscoveryNode newNode(String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(nodeId, nodeId, buildNewFakeTransportAddress(), attributes, CLUSTER_MANAGER_DATA_ROLE, Version.CURRENT);
    }

    final private static Set<DiscoveryNodeRole> CLUSTER_MANAGER_DATA_ROLE = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE))
    );
}

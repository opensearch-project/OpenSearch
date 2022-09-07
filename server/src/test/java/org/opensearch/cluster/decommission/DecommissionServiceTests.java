/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.opensearch.cluster.ClusterState.builder;
import static org.opensearch.cluster.OpenSearchAllocationTestCase.createAllocationService;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;

public class DecommissionServiceTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;
    private AllocationService allocationService;
    private DecommissionService decommissionService;
    private ClusterSettings clusterSettings;

    @Before
    public void setUpService() {
        threadPool = new TestThreadPool("test", Settings.EMPTY);
        clusterService = createClusterService(threadPool);
        allocationService = createAllocationService();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).build();
        logger.info("--> adding cluster manager node on zone_1");
        clusterState = addClusterManagerNodes(clusterState, "zone_1", "node1");
        logger.info("--> adding cluster manager node on zone_2");
        clusterState = addClusterManagerNodes(clusterState, "zone_2", "node6");
        logger.info("--> adding cluster manager node on zone_3");
        clusterState = addClusterManagerNodes(clusterState, "zone_3", "node11");
        logger.info("--> adding four data nodes on zone_1");
        clusterState = addDataNodes(clusterState, "zone_1", "node2", "node3", "node4", "node5");
        logger.info("--> adding four data nodes on zone_2");
        clusterState = addDataNodes(clusterState, "zone_2", "node7", "node8", "node9", "node10");
        logger.info("--> adding four data nodes on zone_3");
        clusterState = addDataNodes(clusterState, "zone_3", "node12", "node13", "node14", "node15");
        clusterState = setLocalNodeAsClusterManagerNode(clusterState, "node1");
        clusterState = setNodesInVotingConfig(
            clusterState,
            clusterState.nodes().get("node1"),
            clusterState.nodes().get("node6"),
            clusterState.nodes().get("node11")
        );
        final ClusterState.Builder builder = builder(clusterState);
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

        final Settings.Builder nodeSettingsBuilder = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "zone_1,zone_2,zone_3");

        clusterSettings = new ClusterSettings(nodeSettingsBuilder.build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        transportService.start();
        transportService.acceptIncomingRequests();

        this.decommissionService = new DecommissionService(
            nodeSettingsBuilder.build(),
            clusterSettings,
            clusterService,
            transportService,
            threadPool,
            allocationService
        );
    }

    @After
    public void shutdownThreadPoolAndClusterService() {
        clusterService.stop();
        threadPool.shutdown();
    }

    @SuppressWarnings("unchecked")
    public void testDecommissioningNotInitiatedForInvalidAttributeName() {
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("rack", "rack-a");
        ActionListener<ClusterStateUpdateResponse> listener = mock(ActionListener.class);
        DecommissioningFailedException e = expectThrows(
            DecommissioningFailedException.class,
            () -> { decommissionService.initiateAttributeDecommissioning(decommissionAttribute, listener, clusterService.state()); }
        );
        assertThat(e.getMessage(), Matchers.endsWith("invalid awareness attribute requested for decommissioning"));
    }

    @SuppressWarnings("unchecked")
    public void testDecommissioningNotInitiatedForInvalidAttributeValue() {
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "random");
        ActionListener<ClusterStateUpdateResponse> listener = mock(ActionListener.class);
        DecommissioningFailedException e = expectThrows(
            DecommissioningFailedException.class,
            () -> { decommissionService.initiateAttributeDecommissioning(decommissionAttribute, listener, clusterService.state()); }
        );
        assertThat(
            e.getMessage(),
            Matchers.endsWith(
                "invalid awareness attribute value requested for decommissioning. Set forced awareness values before to decommission"
            )
        );
    }

    @SuppressWarnings("unchecked")
    public void testDecommissioningNotInitiatedWhenAlreadyDecommissioned() {
        DecommissionAttributeMetadata oldMetadata = new DecommissionAttributeMetadata(
            new DecommissionAttribute("zone", "zone_1"),
            DecommissionStatus.IN_PROGRESS
        );
        final ClusterState.Builder builder = builder(clusterService.state());
        setState(
            clusterService,
            builder.metadata(
                Metadata.builder(clusterService.state().metadata()).putCustom(DecommissionAttributeMetadata.TYPE, oldMetadata).build()
            )
        );
        ActionListener<ClusterStateUpdateResponse> listener = mock(ActionListener.class);
        DecommissioningFailedException e = expectThrows(
            DecommissioningFailedException.class,
            () -> {
                decommissionService.initiateAttributeDecommissioning(
                    new DecommissionAttribute("zone", "zone_2"),
                    listener,
                    clusterService.state()
                );
            }
        );
        assertThat(e.getMessage(), Matchers.endsWith("another request for decommission is in flight, will not process this request"));
    }

    @SuppressWarnings("unchecked")
    public void testDecommissioningInitiatedWhenEnoughClusterManagerNodes() {
        ActionListener<ClusterStateUpdateResponse> listener = mock(ActionListener.class);
        decommissionService.initiateAttributeDecommissioning(new DecommissionAttribute("zone", "zone_3"), listener, clusterService.state());
    }

    @SuppressWarnings("unchecked")
    public void testDecommissioningNotInitiatedWhenNotEnoughClusterManagerNodes() {
        ClusterState state = clusterService.state();
        // shrink voting config
        state = setNodesInVotingConfig(state, state.nodes().get("node1"), state.nodes().get("node11"));
        setState(clusterService, state);
        ActionListener<ClusterStateUpdateResponse> listener = mock(ActionListener.class);
        DecommissioningFailedException e = expectThrows(
            DecommissioningFailedException.class,
            () -> {
                decommissionService.initiateAttributeDecommissioning(
                    new DecommissionAttribute("zone", "zone_3"),
                    listener,
                    clusterService.state()
                );
            }
        );
        assertThat(e.getMessage(), Matchers.endsWith("cannot proceed with decommission request as cluster might go into quorum loss"));
    }

    private ClusterState addDataNodes(ClusterState clusterState, String zone, String... nodeIds) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.nodes());
        org.opensearch.common.collect.List.of(nodeIds).forEach(nodeId -> nodeBuilder.add(newDataNode(nodeId, singletonMap("zone", zone))));
        clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();
        return clusterState;
    }

    private ClusterState addClusterManagerNodes(ClusterState clusterState, String zone, String... nodeIds) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.nodes());
        org.opensearch.common.collect.List.of(nodeIds)
            .forEach(nodeId -> nodeBuilder.add(newClusterManagerNode(nodeId, singletonMap("zone", zone))));
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

    private ClusterState setNodesInVotingConfig(ClusterState clusterState, DiscoveryNode... nodes) {
        final CoordinationMetadata.VotingConfiguration votingConfiguration = CoordinationMetadata.VotingConfiguration.of(nodes);

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

    private static DiscoveryNode newDataNode(String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), attributes, DATA_ROLE, Version.CURRENT);
    }

    private static DiscoveryNode newClusterManagerNode(String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), attributes, CLUSTER_MANAGER_ROLE, Version.CURRENT);
    }

    final private static Set<DiscoveryNodeRole> CLUSTER_MANAGER_ROLE = Collections.unmodifiableSet(
        new HashSet<>(Collections.singletonList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE))
    );

    final private static Set<DiscoveryNodeRole> DATA_ROLE = Collections.unmodifiableSet(
        new HashSet<>(Collections.singletonList(DiscoveryNodeRole.DATA_ROLE))
    );

    private ClusterState removeNodes(ClusterState clusterState, String... nodeIds) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.getNodes());
        org.opensearch.common.collect.List.of(nodeIds).forEach(nodeBuilder::remove);
        return allocationService.disassociateDeadNodes(ClusterState.builder(clusterState).nodes(nodeBuilder).build(), false, "test");
    }
}

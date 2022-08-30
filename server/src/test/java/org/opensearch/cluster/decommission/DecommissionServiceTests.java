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

import java.util.Arrays;
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

    @Override
    public void setUp() throws Exception {
        super.setUp();
        super.setUp();
        threadPool = new TestThreadPool("test", Settings.EMPTY);
        clusterService = createClusterService(threadPool);
        allocationService = createAllocationService();
    }

    @Before
    public void setUpService() {
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
            DecommissionStatus.DECOMMISSION_IN_PROGRESS
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
        assertThat(
            e.getMessage(),
            Matchers.endsWith("one awareness attribute already decommissioned, recommission before triggering another decommission")
        );
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
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), attributes, CLUSTER_MANAGER_DATA_ROLE, Version.CURRENT);
    }

    final private static Set<DiscoveryNodeRole> CLUSTER_MANAGER_DATA_ROLE = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE))
    );
}

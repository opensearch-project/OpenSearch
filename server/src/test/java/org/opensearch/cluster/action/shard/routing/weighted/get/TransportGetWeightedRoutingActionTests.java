/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.action.shard.routing.weighted.get;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingAction;
import org.opensearch.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingRequestBuilder;
import org.opensearch.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingResponse;
import org.opensearch.action.admin.cluster.shards.routing.weighted.get.TransportGetWeightedRoutingAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ActionTestUtils;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.WeightedRouting;
import org.opensearch.cluster.routing.WeightedRoutingService;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;
import static org.mockito.Mockito.mock;

public class TransportGetWeightedRoutingActionTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;
    private WeightedRoutingService weightedRoutingService;
    private TransportGetWeightedRoutingAction transportGetWeightedRoutingAction;
    private ClusterSettings clusterSettings;
    NodeClient client;

    final private static Set<DiscoveryNodeRole> CLUSTER_MANAGER_ROLE = Collections.unmodifiableSet(
        new HashSet<>(Collections.singletonList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE))
    );

    final private static Set<DiscoveryNodeRole> DATA_ROLE = Collections.unmodifiableSet(
        new HashSet<>(Collections.singletonList(DiscoveryNodeRole.DATA_ROLE))
    );

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("test", Settings.EMPTY);
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @Before
    public void setUpService() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).build();
        clusterState = addClusterManagerNodes(clusterState);
        clusterState = addDataNodes(clusterState);
        clusterState = setLocalNode(clusterState, "nodeA1");

        ClusterState.Builder builder = ClusterState.builder(clusterState);
        ClusterServiceUtils.setState(clusterService, builder);

        final MockTransport transport = new MockTransport();
        transportService = transport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> clusterService.state().nodes().get("nodes1"),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );

        Settings.Builder settingsBuilder = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone");

        clusterSettings = new ClusterSettings(settingsBuilder.build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        transportService.start();
        transportService.acceptIncomingRequests();

        this.weightedRoutingService = new WeightedRoutingService(clusterService, threadPool, settingsBuilder.build(), clusterSettings);

        this.transportGetWeightedRoutingAction = new TransportGetWeightedRoutingAction(
            transportService,
            clusterService,
            weightedRoutingService,
            threadPool,
            new ActionFilters(emptySet()),
            mock(IndexNameExpressionResolver.class)
        );
        client = new NodeClient(Settings.EMPTY, threadPool);
    }

    private ClusterState addDataNodes(ClusterState clusterState) {
        clusterState = addDataNodeForAZone(clusterState, "zone_A", "nodeA1", "nodeA2", "nodeA3");
        clusterState = addDataNodeForAZone(clusterState, "zone_B", "nodeB1", "nodeB2", "nodeB3");
        clusterState = addDataNodeForAZone(clusterState, "zone_C", "nodeC1", "nodeC2", "nodeC3");
        return clusterState;
    }

    private ClusterState addClusterManagerNodes(ClusterState clusterState) {
        clusterState = addClusterManagerNodeForAZone(clusterState, "zone_A", "nodeMA");
        clusterState = addClusterManagerNodeForAZone(clusterState, "zone_B", "nodeMB");
        clusterState = addClusterManagerNodeForAZone(clusterState, "zone_C", "nodeMC");
        return clusterState;
    }

    private ClusterState addDataNodeForAZone(ClusterState clusterState, String zone, String... nodeIds) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.nodes());
        List.of(nodeIds)
            .forEach(
                nodeId -> nodeBuilder.add(
                    new DiscoveryNode(
                        nodeId,
                        buildNewFakeTransportAddress(),
                        Collections.singletonMap("zone", zone),
                        DATA_ROLE,
                        Version.CURRENT
                    )
                )
            );
        clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();
        return clusterState;
    }

    private ClusterState addClusterManagerNodeForAZone(ClusterState clusterState, String zone, String... nodeIds) {

        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.nodes());
        List.of(nodeIds)
            .forEach(
                nodeId -> nodeBuilder.add(
                    new DiscoveryNode(
                        nodeId,
                        buildNewFakeTransportAddress(),
                        Collections.singletonMap("zone", zone),
                        CLUSTER_MANAGER_ROLE,
                        Version.CURRENT
                    )
                )
            );
        clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();
        return clusterState;
    }

    private ClusterState setLocalNode(ClusterState clusterState, String nodeId) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.nodes());
        nodeBuilder.localNodeId(nodeId);
        nodeBuilder.clusterManagerNodeId(nodeId);
        clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();
        return clusterState;
    }

    private ClusterState setWeightedRoutingWeights(ClusterState clusterState, Map<String, Double> weights) {
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);
        WeightedRoutingMetadata weightedRoutingMetadata = new WeightedRoutingMetadata(weightedRouting, 0);
        Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
        metadataBuilder.putCustom(WeightedRoutingMetadata.TYPE, weightedRoutingMetadata);
        clusterState = ClusterState.builder(clusterState).metadata(metadataBuilder).build();
        return clusterState;
    }

    public void testGetWeightedRouting_WeightsNotSetInMetadata() {

        final ClusterGetWeightedRoutingRequestBuilder request = new ClusterGetWeightedRoutingRequestBuilder(
            client,
            ClusterGetWeightedRoutingAction.INSTANCE
        );
        request.setAwarenessAttribute("zone");
        ClusterState state = clusterService.state();

        ClusterGetWeightedRoutingResponse response = ActionTestUtils.executeBlocking(transportGetWeightedRoutingAction, request.request());
        assertEquals(response.weights(), null);
    }

    public void testGetWeightedRouting_WeightsSetInMetadata() {
        ClusterGetWeightedRoutingRequestBuilder request = new ClusterGetWeightedRoutingRequestBuilder(
            client,
            ClusterGetWeightedRoutingAction.INSTANCE
        );
        request.setAwarenessAttribute("zone");

        ClusterState state = clusterService.state();
        state = setLocalNode(state, "nodeB1");
        Map<String, Double> weights = Map.of("zone_A", 1.0, "zone_B", 0.0, "zone_C", 1.0);
        state = setWeightedRoutingWeights(state, weights);
        ClusterState.Builder builder = ClusterState.builder(state);
        ClusterServiceUtils.setState(clusterService, builder);

        ClusterGetWeightedRoutingResponse response = ActionTestUtils.executeBlocking(transportGetWeightedRoutingAction, request.request());
        assertEquals(weights, response.weights().weights());
    }

    public void testGetWeightedRoutingLocalWeight_WeightsSetInMetadata() {

        ClusterGetWeightedRoutingRequestBuilder request = new ClusterGetWeightedRoutingRequestBuilder(
            client,
            ClusterGetWeightedRoutingAction.INSTANCE
        );

        request.setRequestLocal(true);
        request.setAwarenessAttribute("zone");

        ClusterState state = clusterService.state();
        state = setLocalNode(state, "nodeB1");
        Map<String, Double> weights = Map.of("zone_A", 1.0, "zone_B", 0.0, "zone_C", 1.0);
        state = setWeightedRoutingWeights(state, weights);
        ClusterState.Builder builder = ClusterState.builder(state);
        ClusterServiceUtils.setState(clusterService, builder);

        ClusterGetWeightedRoutingResponse response = ActionTestUtils.executeBlocking(transportGetWeightedRoutingAction, request.request());
        assertEquals(true, response.getDiscoveredClusterManager());
        assertEquals(weights, response.getWeightedRouting().weights());
    }

    public void testGetWeightedRoutingLocalWeight_WeightsNotSetInMetadata() {

        ClusterGetWeightedRoutingRequestBuilder request = new ClusterGetWeightedRoutingRequestBuilder(
            client,
            ClusterGetWeightedRoutingAction.INSTANCE
        );

        request.setRequestLocal(true);
        request.setAwarenessAttribute("zone");

        ClusterState state = clusterService.state();
        state = setLocalNode(state, "nodeB1");
        ClusterState.Builder builder = ClusterState.builder(state);
        ClusterServiceUtils.setState(clusterService, builder);

        ClusterGetWeightedRoutingResponse response = ActionTestUtils.executeBlocking(transportGetWeightedRoutingAction, request.request());
        assertEquals(null, response.getWeightedRouting());
    }

    @After
    public void shutdown() {
        clusterService.stop();
        threadPool.shutdown();
    }

}

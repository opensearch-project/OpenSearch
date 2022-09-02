/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.action.shard.routing.wrr.get;

import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.shards.routing.wrr.get.ClusterGetWRRWeightsAction;
import org.opensearch.action.admin.cluster.shards.routing.wrr.get.ClusterGetWRRWeightsRequestBuilder;
import org.opensearch.action.admin.cluster.shards.routing.wrr.get.ClusterGetWRRWeightsResponse;
import org.opensearch.action.admin.cluster.shards.routing.wrr.get.TransportGetWRRWeightsAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ActionTestUtils;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WeightedRoundRobinRoutingMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.WRRWeights;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.ClusterServiceUtils;
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
import static org.mockito.Mockito.mock;

public class TransportGetWRRWeightsActionTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;
    private TransportGetWRRWeightsAction transportGetWRRWeightsAction;
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
            Collections.emptySet()

        );

        Settings.Builder settingsBuilder = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone");

        clusterSettings = new ClusterSettings(settingsBuilder.build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        transportService.start();
        transportService.acceptIncomingRequests();

        this.transportGetWRRWeightsAction = new TransportGetWRRWeightsAction(
            settingsBuilder.build(),
            clusterSettings,
            transportService,
            clusterService,
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
        org.opensearch.common.collect.List.of(nodeIds)
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
        org.opensearch.common.collect.List.of(nodeIds)
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

    private ClusterState setWRRWeights(ClusterState clusterState, Map<String, Object> weights) {
        WRRWeights wrrWeight = new WRRWeights("zone", weights);
        WeightedRoundRobinRoutingMetadata wrrMetadata = new WeightedRoundRobinRoutingMetadata(wrrWeight);
        Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
        metadataBuilder.putCustom(WeightedRoundRobinRoutingMetadata.TYPE, wrrMetadata);
        clusterState = ClusterState.builder(clusterState).metadata(metadataBuilder).build();
        return clusterState;
    }

    public void testGetWRR_WeightsNotSetInMetadata() {

        final ClusterGetWRRWeightsRequestBuilder request = new ClusterGetWRRWeightsRequestBuilder(
            client,
            ClusterGetWRRWeightsAction.INSTANCE
        );
        request.setAwarenessAttribute("zone");
        ClusterState state = clusterService.state();

        ClusterGetWRRWeightsResponse response = ActionTestUtils.executeBlocking(transportGetWRRWeightsAction, request.request());
        assertEquals(response.getLocalNodeWeight(), null);
        assertEquals(response.weights(), null);
    }

    public void testGetWRR_WeightsSetInMetadata() {
        ClusterGetWRRWeightsRequestBuilder request = new ClusterGetWRRWeightsRequestBuilder(client, ClusterGetWRRWeightsAction.INSTANCE);
        request.setAwarenessAttribute("zone");

        ClusterState state = clusterService.state();
        state = setLocalNode(state, "nodeB1");
        Map<String, Object> weights = Map.of("zone_A", "1", "zone_B", "0", "zone_C", "1");
        state = setWRRWeights(state, weights);
        ClusterState.Builder builder = ClusterState.builder(state);
        ClusterServiceUtils.setState(clusterService, builder);

        ClusterGetWRRWeightsResponse response = ActionTestUtils.executeBlocking(transportGetWRRWeightsAction, request.request());
        assertEquals(weights, response.weights().weights());
    }

    public void testGetWRRLocalWeight_WeightsSetInMetadata() {

        ClusterGetWRRWeightsRequestBuilder request = new ClusterGetWRRWeightsRequestBuilder(client, ClusterGetWRRWeightsAction.INSTANCE);

        request.setRequestLocal(true);
        request.setAwarenessAttribute("zone");

        ClusterState state = clusterService.state();
        state = setLocalNode(state, "nodeB1");
        Map<String, Object> weights = Map.of("zone_A", "1", "zone_B", "0", "zone_C", "1");
        state = setWRRWeights(state, weights);
        ClusterState.Builder builder = ClusterState.builder(state);
        ClusterServiceUtils.setState(clusterService, builder);

        ClusterGetWRRWeightsResponse response = ActionTestUtils.executeBlocking(transportGetWRRWeightsAction, request.request());
        assertEquals("0", response.getLocalNodeWeight());
    }

    public void testGetWRRLocalWeight_WeightsNotSetInMetadata() {

        ClusterGetWRRWeightsRequestBuilder request = new ClusterGetWRRWeightsRequestBuilder(client, ClusterGetWRRWeightsAction.INSTANCE);

        request.setRequestLocal(true);
        request.setAwarenessAttribute("zone");

        ClusterState state = clusterService.state();
        state = setLocalNode(state, "nodeB1");
        ClusterState.Builder builder = ClusterState.builder(state);
        ClusterServiceUtils.setState(clusterService, builder);

        ClusterGetWRRWeightsResponse response = ActionTestUtils.executeBlocking(transportGetWRRWeightsAction, request.request());
        assertEquals(null, response.getLocalNodeWeight());
    }

    @After
    public void shutdown() {
        clusterService.stop();
        threadPool.shutdown();
    }

}

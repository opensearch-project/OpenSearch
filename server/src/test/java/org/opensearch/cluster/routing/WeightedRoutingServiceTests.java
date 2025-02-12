/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingRequest;
import org.opensearch.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingResponse;
import org.opensearch.action.admin.cluster.shards.routing.weighted.put.ClusterAddWeightedRoutingAction;
import org.opensearch.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingRequestBuilder;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.cluster.decommission.DecommissionAttributeMetadata;
import org.opensearch.cluster.decommission.DecommissionStatus;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class WeightedRoutingServiceTests extends OpenSearchTestCase {
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;
    private WeightedRoutingService weightedRoutingService;
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
        client = new NodeClient(Settings.EMPTY, threadPool);
    }

    @After
    public void shutdown() {
        clusterService.stop();
        threadPool.shutdown();
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

    private ClusterState setDecommissionAttribute(ClusterState clusterState, DecommissionStatus status) {
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "zone_A");
        DecommissionAttributeMetadata decommissionAttributeMetadata = new DecommissionAttributeMetadata(
            decommissionAttribute,
            status,
            randomAlphaOfLength(10)
        );
        Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
        metadataBuilder.decommissionAttributeMetadata(decommissionAttributeMetadata);
        clusterState = ClusterState.builder(clusterState).metadata(metadataBuilder).build();
        return clusterState;
    }

    public void testRegisterWeightedRoutingMetadataWithChangedWeights() throws InterruptedException {
        Map<String, Double> weights = Map.of("zone_A", 1.0, "zone_B", 1.0, "zone_C", 1.0);
        ClusterState state = clusterService.state();
        state = setWeightedRoutingWeights(state, weights);
        ClusterState.Builder builder = ClusterState.builder(state);
        ClusterServiceUtils.setState(clusterService, builder);

        ClusterPutWeightedRoutingRequestBuilder request = new ClusterPutWeightedRoutingRequestBuilder(
            client,
            ClusterAddWeightedRoutingAction.INSTANCE
        );
        WeightedRouting updatedWeightedRouting = new WeightedRouting("zone", Map.of("zone_A", 1.0, "zone_B", 1.0, "zone_C", 0.0));
        request.setWeightedRouting(updatedWeightedRouting);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ActionListener<ClusterStateUpdateResponse> listener = new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                assertTrue(clusterStateUpdateResponse.isAcknowledged());
                assertEquals(updatedWeightedRouting, clusterService.state().metadata().weightedRoutingMetadata().getWeightedRouting());
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("request should not fail");
            }
        };
        weightedRoutingService.registerWeightedRoutingMetadata(request.request(), listener);
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
    }

    public void testRegisterWeightedRoutingMetadataWithSameWeights() throws InterruptedException {
        Map<String, Double> weights = Map.of("zone_A", 1.0, "zone_B", 1.0, "zone_C", 1.0);
        ClusterState state = clusterService.state();
        state = setWeightedRoutingWeights(state, weights);
        ClusterState.Builder builder = ClusterState.builder(state);
        ClusterServiceUtils.setState(clusterService, builder);

        ClusterPutWeightedRoutingRequestBuilder request = new ClusterPutWeightedRoutingRequestBuilder(
            client,
            ClusterAddWeightedRoutingAction.INSTANCE
        );
        WeightedRouting updatedWeightedRouting = new WeightedRouting("zone", weights);
        request.setWeightedRouting(updatedWeightedRouting);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ActionListener<ClusterStateUpdateResponse> listener = new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                assertTrue(clusterStateUpdateResponse.isAcknowledged());
                assertEquals(updatedWeightedRouting, clusterService.state().metadata().weightedRoutingMetadata().getWeightedRouting());
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("request should not fail");
            }
        };
        weightedRoutingService.registerWeightedRoutingMetadata(request.request(), listener);
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
    }

    public void testDeleteWeightedRoutingMetadata() throws InterruptedException {
        Map<String, Double> weights = Map.of("zone_A", 1.0, "zone_B", 1.0, "zone_C", 1.0);
        ClusterState state = clusterService.state();
        state = setWeightedRoutingWeights(state, weights);
        ClusterState.Builder builder = ClusterState.builder(state);
        ClusterServiceUtils.setState(clusterService, builder);

        ClusterDeleteWeightedRoutingRequest clusterDeleteWeightedRoutingRequest = new ClusterDeleteWeightedRoutingRequest("zone");
        clusterDeleteWeightedRoutingRequest.setVersion(0);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ActionListener<ClusterDeleteWeightedRoutingResponse> listener = new ActionListener<ClusterDeleteWeightedRoutingResponse>() {
            @Override
            public void onResponse(ClusterDeleteWeightedRoutingResponse clusterDeleteWeightedRoutingResponse) {
                assertTrue(clusterDeleteWeightedRoutingResponse.isAcknowledged());
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("on failure shouldn't have been called");
            }
        };
        weightedRoutingService.deleteWeightedRoutingMetadata(clusterDeleteWeightedRoutingRequest, listener);
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
    }

    public void testVerifyAwarenessAttribute_InvalidAttributeName() {
        assertThrows(
            "invalid awareness attribute %s requested for updating weighted routing",
            ActionRequestValidationException.class,
            () -> weightedRoutingService.verifyAwarenessAttribute("zone2")
        );
    }

    public void testVerifyAwarenessAttribute_ValidAttributeName() {
        try {
            weightedRoutingService.verifyAwarenessAttribute("zone");
        } catch (Exception e) {
            fail("verify awareness attribute should not fail");
        }
    }

    public void testAddWeightedRoutingFailsWhenWeightsNotSetForAllDiscoveredZones() throws InterruptedException {
        ClusterPutWeightedRoutingRequestBuilder request = new ClusterPutWeightedRoutingRequestBuilder(
            client,
            ClusterAddWeightedRoutingAction.INSTANCE
        );
        Map<String, Double> weights = Map.of("zone_A", 1.0, "zone_C", 1.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);
        request.setWeightedRouting(weightedRouting);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicReference<Exception> exceptionReference = new AtomicReference<>();
        ActionListener<ClusterStateUpdateResponse> listener = new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionReference.set(e);
                countDownLatch.countDown();
            }
        };
        weightedRoutingService.registerWeightedRoutingMetadata(request.request(), listener);
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        MatcherAssert.assertThat("Expected onFailure to be called", exceptionReference.get(), notNullValue());
        MatcherAssert.assertThat(exceptionReference.get(), instanceOf(UnsupportedWeightedRoutingStateException.class));
        MatcherAssert.assertThat(
            exceptionReference.get().getMessage(),
            containsString("weight for [zone_B] is not set and it is part of forced awareness value or a routing node has this attribute.")
        );
    }

    public void testAddWeightedRoutingFailsWhenWeightsForMoreThanHalfIsZero() throws InterruptedException {
        ClusterPutWeightedRoutingRequestBuilder request = new ClusterPutWeightedRoutingRequestBuilder(
            client,
            ClusterAddWeightedRoutingAction.INSTANCE
        );
        Map<String, Double> weights = Map.of("zone_A", 0.0, "zone_B", 0.0, "zone_C", 1.0, "zone_D", 1.0, "zone_E", 1.0, "zone_F", 1.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);
        request.setWeightedRouting(weightedRouting);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicReference<Exception> exceptionReference = new AtomicReference<>();
        ActionListener<ClusterStateUpdateResponse> listener = new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionReference.set(e);
                countDownLatch.countDown();
            }
        };
        weightedRoutingService.registerWeightedRoutingMetadata(request.request(), listener);
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        MatcherAssert.assertThat("Expected onFailure to be called", exceptionReference.get(), notNullValue());
        MatcherAssert.assertThat(exceptionReference.get(), instanceOf(ActionRequestValidationException.class));
        MatcherAssert.assertThat(
            exceptionReference.get().getMessage(),
            containsString("Maximum expected number of routing weights having zero weight is [1]")
        );
    }

    public void testAddWeightedRoutingFailsWhenDecommissionOngoing() throws InterruptedException {
        Map<String, Double> weights = Map.of("zone_A", 1.0, "zone_B", 1.0, "zone_C", 1.0);
        DecommissionStatus status = randomFrom(DecommissionStatus.INIT, DecommissionStatus.IN_PROGRESS, DecommissionStatus.SUCCESSFUL);
        ClusterState state = clusterService.state();
        state = setWeightedRoutingWeights(state, weights);
        state = setDecommissionAttribute(state, status);
        ClusterState.Builder builder = ClusterState.builder(state);
        ClusterServiceUtils.setState(clusterService, builder);

        ClusterPutWeightedRoutingRequestBuilder request = new ClusterPutWeightedRoutingRequestBuilder(
            client,
            ClusterAddWeightedRoutingAction.INSTANCE
        );
        WeightedRouting updatedWeightedRouting = new WeightedRouting("zone", weights);
        request.setWeightedRouting(updatedWeightedRouting);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicReference<Exception> exceptionReference = new AtomicReference<>();
        ActionListener<ClusterStateUpdateResponse> listener = new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionReference.set(e);
                countDownLatch.countDown();
            }
        };
        weightedRoutingService.registerWeightedRoutingMetadata(request.request(), listener);
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        MatcherAssert.assertThat("Expected onFailure to be called", exceptionReference.get(), notNullValue());
        MatcherAssert.assertThat(exceptionReference.get(), instanceOf(UnsupportedWeightedRoutingStateException.class));
        MatcherAssert.assertThat(
            exceptionReference.get().getMessage(),
            containsString("weight for [zone_A] must be set to [0.0] as it is under decommission action")
        );
    }

    public void testAddWeightedRoutingPassesWhenDecommissionFailed() throws InterruptedException {
        Map<String, Double> weights = Map.of("zone_A", 1.0, "zone_B", 1.0, "zone_C", 1.0);
        DecommissionStatus status = DecommissionStatus.FAILED;
        ClusterState state = clusterService.state();
        state = setWeightedRoutingWeights(state, weights);
        state = setDecommissionAttribute(state, status);
        ClusterState.Builder builder = ClusterState.builder(state);
        ClusterServiceUtils.setState(clusterService, builder);

        ClusterPutWeightedRoutingRequestBuilder request = new ClusterPutWeightedRoutingRequestBuilder(
            client,
            ClusterAddWeightedRoutingAction.INSTANCE
        );
        WeightedRouting updatedWeightedRouting = new WeightedRouting("zone", weights);
        request.setWeightedRouting(updatedWeightedRouting);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicReference<Exception> exceptionReference = new AtomicReference<>();
        ActionListener<ClusterStateUpdateResponse> listener = new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                assertTrue(clusterStateUpdateResponse.isAcknowledged());
                assertEquals(updatedWeightedRouting, clusterService.state().metadata().weightedRoutingMetadata().getWeightedRouting());
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {}
        };
        weightedRoutingService.registerWeightedRoutingMetadata(request.request(), listener);
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
    }

    public void testAddWeightedRoutingPassesWhenWeightOfDecommissionedAttributeStillZero() throws InterruptedException {
        Map<String, Double> weights = Map.of("zone_A", 0.0, "zone_B", 1.0, "zone_C", 1.0);
        DecommissionStatus status = DecommissionStatus.SUCCESSFUL;
        ClusterState state = clusterService.state();
        state = setWeightedRoutingWeights(state, weights);
        state = setDecommissionAttribute(state, status);
        ClusterState.Builder builder = ClusterState.builder(state);
        ClusterServiceUtils.setState(clusterService, builder);

        ClusterPutWeightedRoutingRequestBuilder request = new ClusterPutWeightedRoutingRequestBuilder(
            client,
            ClusterAddWeightedRoutingAction.INSTANCE
        );
        Map<String, Double> updatedWeights = Map.of("zone_A", 0.0, "zone_B", 2.0, "zone_C", 1.0);
        WeightedRouting updatedWeightedRouting = new WeightedRouting("zone", updatedWeights);
        request.setWeightedRouting(updatedWeightedRouting);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ActionListener<ClusterStateUpdateResponse> listener = new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                assertTrue(clusterStateUpdateResponse.isAcknowledged());
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {}
        };
        weightedRoutingService.registerWeightedRoutingMetadata(request.request(), listener);
        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertEquals(updatedWeightedRouting, clusterService.state().metadata().weightedRoutingMetadata().getWeightedRouting());
    }
}

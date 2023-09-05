/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingResponse;
import org.opensearch.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingResponse;
import org.opensearch.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.settings.Settings;
import org.opensearch.discovery.ClusterManagerNotDiscoveredException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.transport.MockTransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, minNumDataNodes = 3)
public class WeightedRoutingIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockRepository.Plugin.class);
    }

    public void testPutWeightedRouting() {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        logger.info("--> starting 6 nodes on different zones");
        int nodeCountPerAZ = 2;

        logger.info("--> starting a dedicated cluster manager node");
        internalCluster().startClusterManagerOnlyNode(Settings.builder().put(commonSettings).build());

        logger.info("--> starting 1 nodes on zones 'a' & 'b' & 'c'");
        List<String> nodes_in_zone_a = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build()
        );
        List<String> nodes_in_zone_b = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build()
        );
        List<String> nodes_in_zone_c = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").build()
        );

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("7").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ensureGreen();

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 2.0, "c", 3.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

        ClusterPutWeightedRoutingResponse response = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(-1)
            .get();
        assertEquals(response.isAcknowledged(), true);

        // put call made on a data node in zone a
        response = internalCluster().client(randomFrom(nodes_in_zone_a.get(0), nodes_in_zone_a.get(1)))
            .admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(0)
            .get();
        assertEquals(response.isAcknowledged(), true);
    }

    public void testPutWeightedRouting_InvalidAwarenessAttribute() {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        internalCluster().startNodes(
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").build()
        );

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("3").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ensureGreen();

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 2.0, "c", 3.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone1", weights);

        assertThrows(
            IllegalArgumentException.class,
            () -> client().admin().cluster().prepareWeightedRouting().setWeightedRouting(weightedRouting).get()
        );
    }

    public void testPutWeightedRouting_MoreThanOneZoneHasZeroWeight() {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        internalCluster().startNodes(
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").build()
        );

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("3").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ensureGreen();

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 0.0, "c", 0.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone1", weights);

        assertThrows(
            IllegalArgumentException.class,
            () -> client().admin().cluster().prepareWeightedRouting().setWeightedRouting(weightedRouting).get()
        );
    }

    public void testGetWeightedRouting_WeightsNotSet() {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        internalCluster().startNodes(
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").build()
        );

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("3").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ensureGreen();

        ClusterGetWeightedRoutingResponse weightedRoutingResponse = client().admin()
            .cluster()
            .prepareGetWeightedRouting()
            .setAwarenessAttribute("zone")
            .get();
        assertNull(weightedRoutingResponse.weights());
        assertNull(weightedRoutingResponse.getDiscoveredClusterManager());
    }

    public void testGetWeightedRouting_WeightsAreSet() throws IOException {

        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        int nodeCountPerAZ = 2;

        logger.info("--> starting a dedicated cluster manager node");
        internalCluster().startClusterManagerOnlyNode(Settings.builder().put(commonSettings).build());

        logger.info("--> starting 2 nodes on zones 'a' & 'b' & 'c'");
        List<String> nodes_in_zone_a = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build()
        );
        List<String> nodes_in_zone_b = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build()
        );
        List<String> nodes_in_zone_c = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").build()
        );

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("7").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ensureGreen();

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 2.0, "c", 3.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);
        // put api call to set weights
        ClusterPutWeightedRoutingResponse response = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(-1)
            .get();
        assertEquals(response.isAcknowledged(), true);

        // get api call to fetch weights
        ClusterGetWeightedRoutingResponse weightedRoutingResponse = client().admin()
            .cluster()
            .prepareGetWeightedRouting()
            .setAwarenessAttribute("zone")
            .get();
        assertEquals(weightedRouting, weightedRoutingResponse.weights());
        assertTrue(weightedRoutingResponse.getDiscoveredClusterManager());

        // get api to fetch local weighted routing for a node in zone a
        weightedRoutingResponse = internalCluster().client(randomFrom(nodes_in_zone_a.get(0), nodes_in_zone_a.get(1)))
            .admin()
            .cluster()
            .prepareGetWeightedRouting()
            .setAwarenessAttribute("zone")
            .setRequestLocal(true)
            .get();
        assertEquals(weightedRouting, weightedRoutingResponse.weights());
        assertTrue(weightedRoutingResponse.getDiscoveredClusterManager());

        // get api to fetch local weighted routing for a node in zone b
        weightedRoutingResponse = internalCluster().client(randomFrom(nodes_in_zone_b.get(0), nodes_in_zone_b.get(1)))
            .admin()
            .cluster()
            .prepareGetWeightedRouting()
            .setAwarenessAttribute("zone")
            .setRequestLocal(true)
            .get();
        assertEquals(weightedRouting, weightedRoutingResponse.weights());
        assertTrue(weightedRoutingResponse.getDiscoveredClusterManager());

        // get api to fetch local weighted routing for a node in zone c
        weightedRoutingResponse = internalCluster().client(randomFrom(nodes_in_zone_c.get(0), nodes_in_zone_c.get(1)))
            .admin()
            .cluster()
            .prepareGetWeightedRouting()
            .setAwarenessAttribute("zone")
            .setRequestLocal(true)
            .get();
        assertEquals(weightedRouting, weightedRoutingResponse.weights());
        assertTrue(weightedRoutingResponse.getDiscoveredClusterManager());

    }

    public void testGetWeightedRouting_ClusterManagerNotDiscovered() throws Exception {

        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.fault_detection.leader_check.timeout", 10000 + "ms")
            .put("cluster.fault_detection.leader_check.retry_count", 1)
            .build();

        int nodeCountPerAZ = 1;

        logger.info("--> starting a dedicated cluster manager node");
        String clusterManager = internalCluster().startClusterManagerOnlyNode(Settings.builder().put(commonSettings).build());

        logger.info("--> starting 2 nodes on zones 'a' & 'b' & 'c'");
        List<String> nodes_in_zone_a = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build()
        );
        List<String> nodes_in_zone_b = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build()
        );
        List<String> nodes_in_zone_c = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").build()
        );

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("4").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ensureGreen();

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 2.0, "c", 3.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);
        // put api call to set weights
        ClusterPutWeightedRoutingResponse response = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(-1)
            .get();
        assertEquals(response.isAcknowledged(), true);

        Set<String> nodesInOneSide = Stream.of(nodes_in_zone_a.get(0), nodes_in_zone_b.get(0), nodes_in_zone_c.get(0))
            .collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInOtherSide = Stream.of(clusterManager).collect(Collectors.toCollection(HashSet::new));

        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(nodesInOneSide, nodesInOtherSide),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(networkDisruption);

        logger.info("--> network disruption is started");
        networkDisruption.startDisrupting();

        // wait for leader checker to fail
        Thread.sleep(13000);

        // get api to fetch local weighted routing for a node in zone a or b
        ClusterGetWeightedRoutingResponse weightedRoutingResponse = internalCluster().client(
            randomFrom(nodes_in_zone_a.get(0), nodes_in_zone_b.get(0))
        ).admin().cluster().prepareGetWeightedRouting().setAwarenessAttribute("zone").setRequestLocal(true).get();
        assertEquals(weightedRouting, weightedRoutingResponse.weights());
        assertFalse(weightedRoutingResponse.getDiscoveredClusterManager());
        logger.info("--> network disruption is stopped");
        networkDisruption.stopDisrupting();

    }

    public void testWeightedRoutingMetadataOnOSProcessRestart() throws Exception {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        internalCluster().startNodes(
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").build()
        );

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("3").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ensureGreen();

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 2.0, "c", 3.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);
        // put api call to set weights
        ClusterPutWeightedRoutingResponse response = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(-1)
            .get();
        assertEquals(response.isAcknowledged(), true);

        ensureStableCluster(3);

        // routing weights are set in cluster metadata
        assertNotNull(internalCluster().clusterService().state().metadata().weightedRoutingMetadata());

        ensureGreen();

        // Restart a random data node and check that OS process comes healthy
        internalCluster().restartRandomDataNode();
        ensureGreen();
        assertNotNull(internalCluster().clusterService().state().metadata().weightedRoutingMetadata());
    }

    public void testWeightedRoutingOnOSProcessRestartAfterWeightDelete() throws Exception {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        internalCluster().startNodes(
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").build()
        );

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("3").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ensureGreen();

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 2.0, "c", 3.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);
        // put api call to set weights
        ClusterPutWeightedRoutingResponse response = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(-1)
            .get();
        assertEquals(response.isAcknowledged(), true);

        ensureStableCluster(3);

        // routing weights are set in cluster metadata
        assertNotNull(internalCluster().clusterService().state().metadata().weightedRoutingMetadata());

        ensureGreen();

        // delete weighted routing metadata
        ClusterDeleteWeightedRoutingResponse deleteResponse = client().admin().cluster().prepareDeleteWeightedRouting().setVersion(0).get();
        assertTrue(deleteResponse.isAcknowledged());

        // Restart a random data node and check that OS process comes healthy
        internalCluster().restartRandomDataNode();
        ensureGreen();
        assertNotNull(internalCluster().clusterService().state().metadata().weightedRoutingMetadata());
    }

    public void testDeleteWeightedRouting_WeightsNotSet() {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        internalCluster().startNodes(
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").build()
        );

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("3").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ensureGreen();

        assertNull(internalCluster().clusterService().state().metadata().weightedRoutingMetadata());

        // delete weighted routing metadata
        ResourceNotFoundException exception = expectThrows(
            ResourceNotFoundException.class,
            () -> client().admin().cluster().prepareDeleteWeightedRouting().setVersion(-1).get()
        );
        assertEquals(RestStatus.NOT_FOUND, exception.status());
    }

    public void testDeleteWeightedRouting_WeightsAreSet() throws IOException {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        internalCluster().startNodes(
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").build()
        );

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("3").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ensureGreen();

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 2.0, "c", 3.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

        // put api call to set weights
        ClusterPutWeightedRoutingResponse response = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(-1)
            .get();
        assertTrue(response.isAcknowledged());
        assertNotNull(internalCluster().clusterService().state().metadata().weightedRoutingMetadata());

        // delete weighted routing metadata
        ClusterDeleteWeightedRoutingResponse deleteResponse = client().admin().cluster().prepareDeleteWeightedRouting().setVersion(0).get();
        assertTrue(deleteResponse.isAcknowledged());
    }

    public void testPutAndDeleteWithVersioning() throws Exception {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        logger.info("--> starting 6 nodes on different zones");
        int nodeCountPerAZ = 2;

        logger.info("--> starting a dedicated cluster manager node");
        internalCluster().startClusterManagerOnlyNode(Settings.builder().put(commonSettings).build());

        logger.info("--> starting 1 nodes on zones 'a' & 'b' & 'c'");
        internalCluster().startDataOnlyNodes(nodeCountPerAZ, Settings.builder().put(commonSettings).put("node.attr.zone", "a").build());
        internalCluster().startDataOnlyNodes(nodeCountPerAZ, Settings.builder().put(commonSettings).put("node.attr.zone", "b").build());
        internalCluster().startDataOnlyNodes(nodeCountPerAZ, Settings.builder().put(commonSettings).put("node.attr.zone", "c").build());

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("7").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ensureGreen();

        logger.info("--> setting shard routing weights for weighted round robin");

        Map<String, Double> weights = Map.of("a", 1.0, "b", 2.0, "c", 3.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);
        ClusterPutWeightedRoutingResponse response = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(-1)
            .get();
        assertTrue(response.isAcknowledged());
        assertNotNull(internalCluster().clusterService().state().metadata().weightedRoutingMetadata());

        // update weights api call with correct version number
        weights = Map.of("a", 1.0, "b", 2.0, "c", 4.0);
        weightedRouting = new WeightedRouting("zone", weights);
        response = client().admin().cluster().prepareWeightedRouting().setWeightedRouting(weightedRouting).setVersion(0).get();
        assertTrue(response.isAcknowledged());

        // update weights api call with incorrect version number
        weights = Map.of("a", 1.0, "b", 2.0, "c", 4.0);
        WeightedRouting weightedRouting1 = new WeightedRouting("zone", weights);
        UnsupportedWeightedRoutingStateException exception = expectThrows(
            UnsupportedWeightedRoutingStateException.class,
            () -> client().admin().cluster().prepareWeightedRouting().setWeightedRouting(weightedRouting1).setVersion(100).get()
        );
        assertEquals(exception.status(), RestStatus.CONFLICT);

        // get weights call
        ClusterGetWeightedRoutingResponse weightedRoutingResponse = client().admin()
            .cluster()
            .prepareGetWeightedRouting()
            .setAwarenessAttribute("zone")
            .get();

        // update weights call using version returned by get api call
        weights = Map.of("a", 1.0, "b", 2.0, "c", 5.0);
        weightedRouting = new WeightedRouting("zone", weights);
        response = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(weightedRoutingResponse.getVersion())
            .get();
        assertTrue(response.isAcknowledged());
        assertNotNull(internalCluster().clusterService().state().metadata().weightedRoutingMetadata());

        // delete weights by awareness attribute
        ClusterDeleteWeightedRoutingResponse deleteResponse = client().admin()
            .cluster()
            .prepareDeleteWeightedRouting()
            .setAwarenessAttribute("zone")
            .setVersion(2)
            .get();
        assertTrue(deleteResponse.isAcknowledged());

        // update weights again and make sure that version number got updated on delete
        weights = Map.of("a", 1.0, "b", 2.0, "c", 6.0);
        weightedRouting = new WeightedRouting("zone", weights);
        response = client().admin().cluster().prepareWeightedRouting().setWeightedRouting(weightedRouting).setVersion(3).get();
        assertTrue(response.isAcknowledged());
        assertNotNull(internalCluster().clusterService().state().metadata().weightedRoutingMetadata());

        // delete weights
        deleteResponse = client().admin().cluster().prepareDeleteWeightedRouting().setVersion(4).get();
        assertTrue(deleteResponse.isAcknowledged());

        // delete weights call, incorrect version number
        UnsupportedWeightedRoutingStateException deleteException = expectThrows(
            UnsupportedWeightedRoutingStateException.class,
            () -> client().admin().cluster().prepareDeleteWeightedRouting().setVersion(7).get()
        );
        assertEquals(RestStatus.CONFLICT, deleteException.status());
    }

    public void testClusterHealthResponseWithEnsureNodeWeighedInParam() throws Exception {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        logger.info("--> starting 3 nodes on different zones");
        int nodeCountPerAZ = 1;

        logger.info("--> starting a dedicated cluster manager node");
        String clusterManager = internalCluster().startClusterManagerOnlyNode(Settings.builder().put(commonSettings).build());

        logger.info("--> starting 2 nodes on zones 'a' & 'b' & 'c'");
        List<String> nodes_in_zone_a = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build()
        );
        List<String> nodes_in_zone_b = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build()
        );
        List<String> nodes_in_zone_c = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").build()
        );

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("4").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ensureGreen();

        logger.info("--> setting shard routing weights for weighted round robin");

        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);
        ClusterPutWeightedRoutingResponse response = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(-1)
            .get();
        assertTrue(response.isAcknowledged());
        assertNotNull(internalCluster().clusterService().state().metadata().weightedRoutingMetadata());

        // Check cluster health for weighed in node, health check should return a response with 200 status code
        ClusterHealthResponse nodeLocalHealth = client(nodes_in_zone_a.get(0)).admin()
            .cluster()
            .prepareHealth()
            .setLocal(true)
            .setEnsureNodeWeighedIn(true)
            .get();
        assertFalse(nodeLocalHealth.isTimedOut());

        // Check cluster health for weighed away node, health check should respond with an exception
        NodeWeighedAwayException ex = expectThrows(
            NodeWeighedAwayException.class,
            () -> client(nodes_in_zone_c.get(0)).admin().cluster().prepareHealth().setLocal(true).setEnsureNodeWeighedIn(true).get()
        );
        assertTrue(ex.getMessage().contains("local node is weighed away"));

        logger.info("--> running cluster health on an index that does not exists");
        ClusterHealthResponse healthResponse = client(nodes_in_zone_c.get(0)).admin()
            .cluster()
            .prepareHealth("test1")
            .setLocal(true)
            .setEnsureNodeWeighedIn(true)
            .setTimeout("1s")
            .execute()
            .actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(true));
        assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse.getIndices().isEmpty(), equalTo(true));

        Set<String> nodesInOneSide = Stream.of(nodes_in_zone_a.get(0), nodes_in_zone_b.get(0), nodes_in_zone_c.get(0))
            .collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInOtherSide = Stream.of(clusterManager).collect(Collectors.toCollection(HashSet::new));

        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(nodesInOneSide, nodesInOtherSide),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(networkDisruption);

        logger.info("--> network disruption is started");
        networkDisruption.startDisrupting();

        // wait for leader checker to fail
        Thread.sleep(13000);

        // Check cluster health for weighed in node when cluster manager is not discovered, health check should
        // return a response with 503 status code
        assertThrows(
            ClusterManagerNotDiscoveredException.class,
            () -> client(nodes_in_zone_a.get(0)).admin().cluster().prepareHealth().setLocal(true).setEnsureNodeWeighedIn(true).get()
        );

        // Check cluster health for weighed away node when cluster manager is not discovered, health check should
        // return a response with 503 status code
        assertThrows(
            ClusterManagerNotDiscoveredException.class,
            () -> client(nodes_in_zone_c.get(0)).admin().cluster().prepareHealth().setLocal(true).setEnsureNodeWeighedIn(true).get()
        );
        networkDisruption.stopDisrupting();
        Thread.sleep(1000);

        // delete weights
        ClusterDeleteWeightedRoutingResponse deleteResponse = client().admin().cluster().prepareDeleteWeightedRouting().setVersion(0).get();
        assertTrue(deleteResponse.isAcknowledged());

        // Check local cluster health
        nodeLocalHealth = client(nodes_in_zone_c.get(0)).admin()
            .cluster()
            .prepareHealth()
            .setLocal(true)
            .setEnsureNodeWeighedIn(true)
            .get();
        assertFalse(nodeLocalHealth.isTimedOut());
        assertTrue(nodeLocalHealth.hasDiscoveredClusterManager());
    }
}

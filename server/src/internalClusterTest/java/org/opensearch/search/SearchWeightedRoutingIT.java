/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.junit.Assert;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingResponse;
import org.opensearch.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.WeightedRouting;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.search.stats.SearchStats;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.test.transport.MockTransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, minNumDataNodes = 3)
public class SearchWeightedRoutingIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockRepository.Plugin.class);
    }

    public void testSearchWithWRRShardRouting() throws IOException {
        Settings commonSettings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone" + ".values", "a,b,c")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .build();

        logger.info("--> starting 6 nodes on different zones");
        List<String> nodes = internalCluster().startNodes(
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").build()
        );

        String A_0 = nodes.get(0);
        String B_0 = nodes.get(1);
        String B_1 = nodes.get(2);
        String A_1 = nodes.get(3);
        String C_0 = nodes.get(4);
        String C_1 = nodes.get(5);

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("6").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 10).put("index.number_of_replicas", 2))
        );
        ensureGreen();
        logger.info("--> creating indices for test");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test_" + i).setId("" + i).setSource("field_" + i, "value_" + i).get();
        }

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

        ClusterPutWeightedRoutingResponse response = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .get();
        assertEquals(response.isAcknowledged(), true);

        Set<String> hitNodes = new HashSet<>();
        // making search requests
        for (int i = 0; i < 50; i++) {
            SearchResponse searchResponse = internalCluster().client(randomFrom(A_0, A_1, B_0, B_1))
                .prepareSearch()
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
            assertEquals(searchResponse.getFailedShards(), 0);
            for (int j = 0; j < searchResponse.getHits().getHits().length; j++) {
                hitNodes.add(searchResponse.getHits().getAt(j).getShard().getNodeId());
            }
        }
        // search should not go to nodes in zone c with weight zero in case
        // shard copies are available in other zones
        assertThat(hitNodes.size(), lessThanOrEqualTo(4));
        DiscoveryNodes dataNodes = internalCluster().clusterService().state().nodes();
        List<String> nodeIdsFromZoneWithWeightZero = new ArrayList<>();
        for (DiscoveryNode node : dataNodes) {
            if (node.getAttributes().get("zone").equals("c")) {
                nodeIdsFromZoneWithWeightZero.add(node.getId());
            }
        }
        for (String nodeId : nodeIdsFromZoneWithWeightZero) {
            assertFalse(hitNodes.contains(nodeId));
        }

        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().execute().actionGet();
        for (NodeStats stat : nodeStats.getNodes()) {
            SearchStats.Stats searchStats = stat.getIndices().getSearch().getTotal();
            if (stat.getNode().getAttributes().get("zone").equals("c")) {
                assertEquals(0, searchStats.getQueryCount());
                assertEquals(0, searchStats.getFetchCount());

            } else {
                Assert.assertTrue(searchStats.getQueryCount() > 0L);
                Assert.assertTrue(searchStats.getFetchCount() > 0L);
            }
        }

        logger.info("--> deleted shard routing weights for weighted round robin");

        ClusterDeleteWeightedRoutingResponse deleteResponse = client().admin().cluster().prepareDeleteWeightedRouting().get();
        assertEquals(deleteResponse.isAcknowledged(), true);

        hitNodes = new HashSet<>();
        // making search requests
        for (int i = 0; i < 100; i++) {
            SearchResponse searchResponse = internalCluster().client(randomFrom(A_0, A_1, B_0, B_1))
                .prepareSearch()
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
            assertEquals(searchResponse.getFailedShards(), 0);
            for (int j = 0; j < searchResponse.getHits().getHits().length; j++) {
                hitNodes.add(searchResponse.getHits().getAt(j).getShard().getNodeId());
            }
        }

        // Check shard routing requests hit data nodes in zone c
        for (String nodeId : nodeIdsFromZoneWithWeightZero) {
            assertFalse(!hitNodes.contains(nodeId));
        }
        nodeStats = client().admin().cluster().prepareNodesStats().execute().actionGet();

        for (NodeStats stat : nodeStats.getNodes()) {
            SearchStats.Stats searchStats = stat.getIndices().getSearch().getTotal();
            Assert.assertTrue(searchStats.getQueryCount() > 0L);
            Assert.assertTrue(searchStats.getFetchCount() > 0L);
        }
    }

    /**
     * Shard routing request is served by data nodes in az with weight set as 0,
     * in case shard copies are not available in other azs
     * This is tested by setting up a 4 node cluster with one data node per az.
     * Weighted shard routing weight is set as 0 for az-c.
     * Data nodes in zone a and b are stopped,
     * assertions are put to make sure shard search requests do not fail.
     * @throws IOException
     */
    public void testFailOpenByStoppingDataNodes() throws IOException {

        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        int nodeCountPerAZ = 1;

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
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("4").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ensureGreen();

        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 2))
        );
        ensureGreen();

        logger.info("--> creating indices for test");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test").setId("" + i).setSource("field_" + i, "value_" + i).get();
        }
        refresh("test");

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

        ClusterPutWeightedRoutingResponse response = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .get();
        assertEquals(response.isAcknowledged(), true);

        logger.info("--> data nodes in zone a and b are stopped");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodes_in_zone_a.get(0)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodes_in_zone_b.get(0)));
        ensureStableCluster(2);

        Set<String> hitNodes = new HashSet<>();
        // making search requests
        for (int i = 0; i < 50; i++) {
            SearchResponse searchResponse = internalCluster().smartClient()
                .prepareSearch("test")
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
            // assert that searches do not fail and are served by data node in zone c
            assertEquals(searchResponse.getFailedShards(), 0);
            for (int j = 0; j < searchResponse.getHits().getHits().length; j++) {
                hitNodes.add(searchResponse.getHits().getAt(j).getShard().getNodeId());
            }
        }

        ImmutableOpenMap<String, DiscoveryNode> dataNodes = internalCluster().clusterService().state().nodes().getDataNodes();
        String dataNodeInZoneCID = null;

        for (Iterator<DiscoveryNode> it = dataNodes.valuesIt(); it.hasNext();) {
            DiscoveryNode node = it.next();
            if (node.getAttributes().get("zone").equals("c")) {
                dataNodeInZoneCID = node.getId();
                break;
            }
        }

        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().execute().actionGet();
        for (NodeStats stat : nodeStats.getNodes()) {
            SearchStats.Stats searchStats = stat.getIndices().getSearch().getTotal();
            if (stat.getNode().isDataNode()) {
                Assert.assertTrue(searchStats.getQueryCount() > 0L);
                Assert.assertTrue(searchStats.getFetchCount() > 0L);
                assertTrue(stat.getNode().getId().equals(dataNodeInZoneCID));
            }
        }
    }

    /**
     * Shard routing request is served by data nodes in az with weight set as 0,
     * in case shard copies are not available in other azs.
     * This is tested by setting up a 4 node cluster with one data node per az.
     * Weighted shard routing weight is set as 0 for az-c.
     * Indices are created with one replica copy and network disruption is introduced,
     * which makes node in zone a unresponsive.
     * Since there are two copies of a shard, there can be few shards for which copy doesn't exist in zone b.
     * Assertions are put to make sure such shard search requests are served by data node in zone c.
     * @throws IOException
     */
    public void testFailOpenWithUnresponsiveNetworkDisruption() throws Exception {

        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        int nodeCountPerAZ = 1;

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
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("4").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ensureGreen();

        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 10).put("index" + ".number_of_replicas", 1))
        );
        ensureGreen();
        logger.info("--> creating indices for test");
        for (int i = 0; i < 50; i++) {
            client().prepareIndex("test").setId("" + i).setSource("field_" + i, "value_" + i).get();
        }
        refresh("test");

        ClusterState state1 = internalCluster().clusterService().state();

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

        ClusterPutWeightedRoutingResponse response = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .get();
        assertEquals(response.isAcknowledged(), true);

        logger.info("--> creating network partition disruption");
        final String clusterManagerNode1 = internalCluster().getClusterManagerName();
        Set<String> nodesInOneSide = Stream.of(clusterManagerNode1, nodes_in_zone_b.get(0), nodes_in_zone_c.get(0))
            .collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInOtherSide = Stream.of(nodes_in_zone_a.get(0)).collect(Collectors.toCollection(HashSet::new));

        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(nodesInOneSide, nodesInOtherSide),
            NetworkDisruption.UNRESPONSIVE
        );
        internalCluster().setDisruptionScheme(networkDisruption);

        logger.info("--> network disruption is started");
        networkDisruption.startDisrupting();

        Set<String> hitNodes = new HashSet<>();
        Future<SearchResponse>[] responses = new Future[50];
        logger.info("--> making search requests");
        for (int i = 0; i < 50; i++) {
            responses[i] = internalCluster().client(nodes_in_zone_b.get(0))
                .prepareSearch("test")
                .setQuery(QueryBuilders.matchAllQuery())
                .execute();
        }

        logger.info("--> network disruption is stopped");
        networkDisruption.stopDisrupting();

        for (int i = 0; i < 50; i++) {
            try {
                SearchResponse searchResponse = responses[i].get();
                assertEquals(searchResponse.getFailedShards(), 0);
                for (int j = 0; j < searchResponse.getHits().getHits().length; j++) {
                    hitNodes.add(searchResponse.getHits().getAt(j).getShard().getNodeId());
                }
            } catch (Exception t) {
                fail("search should not fail");
            }
        }

        ImmutableOpenMap<String, DiscoveryNode> dataNodes = internalCluster().clusterService().state().nodes().getDataNodes();
        String dataNodeInZoneAId = null;
        String dataNodeInZoneBId = null;
        String dataNodeInZoneCId = null;
        for (Iterator<DiscoveryNode> it = dataNodes.valuesIt(); it.hasNext();) {
            DiscoveryNode node = it.next();
            switch (node.getAttributes().get("zone")) {
                case "a":
                    dataNodeInZoneAId = node.getId();
                    break;
                case "b":
                    dataNodeInZoneBId = node.getId();
                    break;
                case "c":
                    dataNodeInZoneCId = node.getId();
            }
        }

        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().execute().actionGet();
        for (NodeStats stat : nodeStats.getNodes()) {
            SearchStats.Stats searchStats = stat.getIndices().getSearch().getTotal();
            if (stat.getNode().isDataNode()) {
                if (stat.getNode().getId().equals(dataNodeInZoneBId)) {
                    Assert.assertTrue(searchStats.getQueryCount() > 0L);
                    Assert.assertTrue(searchStats.getFetchCount() > 0L);
                } else if (stat.getNode().getId().equals(dataNodeInZoneCId)) {
                    Assert.assertTrue(searchStats.getQueryCount() > 0L);
                } else {
                    // search requests do not hit data node in zone a
                    assertEquals(0, searchStats.getQueryCount());
                    assertEquals(0, searchStats.getFetchCount());
                }
            }
        }
    }

    /**
     * Shard routing request is served by data nodes in az with weight set as 0,
     * in case shard copies are not available in other azs.
     * This is tested by setting up a 4 node cluster with one data node per az.
     * Weighted shard routing weight is set as 0 for az-c.
     * Indices are created with one replica copy and network disruption is introduced,
     * which makes node in zone a unresponsive.
     * Since there are two copies of a shard, there can be few shards for which copy doesn't exist in zone b.
     * Assertions are put to make sure such shard search requests are served by data node in zone c.
     * @throws IOException
     */
    public void testSearchAggregationFailOpen_WithUnresponsiveNetworkDisruption() throws Exception {

        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .build();

        int nodeCountPerAZ = 1;

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
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("4").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ensureGreen();

        assertAcked(
            prepareCreate("index").setMapping("f", "type=keyword")
                .setSettings(Settings.builder().put("index" + ".number_of_shards", 10).put("index" + ".number_of_replicas", 1))
        );

        // assertAcked(prepareCreate("index").setMapping("f", "type=keyword").get());
        int numDocs = randomIntBetween(1, 20);
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; ++i) {
            docs.add(client().prepareIndex("index").setSource("f", Integer.toString(i / 3)));
        }
        indexRandom(true, docs);

        ensureGreen();

        refresh("index");

        ClusterState state1 = internalCluster().clusterService().state();

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

        ClusterPutWeightedRoutingResponse response = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .get();
        assertEquals(response.isAcknowledged(), true);

        logger.info("--> creating network partition disruption");
        final String clusterManagerNode1 = internalCluster().getClusterManagerName();
        Set<String> nodesInOneSide = Stream.of(clusterManagerNode1, nodes_in_zone_b.get(0), nodes_in_zone_c.get(0))
            .collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInOtherSide = Stream.of(nodes_in_zone_a.get(0)).collect(Collectors.toCollection(HashSet::new));

        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(nodesInOneSide, nodesInOtherSide),
            NetworkDisruption.UNRESPONSIVE
        );
        internalCluster().setDisruptionScheme(networkDisruption);

        logger.info("--> network disruption is started");
        networkDisruption.startDisrupting();

        Set<String> hitNodes = new HashSet<>();
        Future<SearchResponse>[] responses = new Future[50];
        int size = 17;
        logger.info("--> making search requests");
        for (int i = 0; i < 5; i++) {
            responses[i] = client().prepareSearch("index").setSize(size).addAggregation(terms("f").field("f")).execute();
        }

        logger.info("--> network disruption is stopped");
        networkDisruption.stopDisrupting();

        for (int i = 0; i < 5; i++) {
            try {
                SearchResponse searchResponse = responses[i].get();
                // assertSearchResponse(searchResponse);
                Aggregations aggregations = searchResponse.getAggregations();
                assertNotNull(aggregations);
                Terms terms = aggregations.get("f");
                assertEquals(Math.min(numDocs, 3L), terms.getBucketByKey("0").getDocCount());
                assertEquals(0, searchResponse.getFailedShards());
                for (int j = 0; j < searchResponse.getHits().getHits().length; j++) {
                    hitNodes.add(searchResponse.getHits().getAt(j).getShard().getNodeId());
                }
            } catch (Exception t) {
                fail("search should not fail");
            }
        }

        ImmutableOpenMap<String, DiscoveryNode> dataNodes = internalCluster().clusterService().state().nodes().getDataNodes();
        String dataNodeInZoneAId = null;
        String dataNodeInZoneBId = null;
        String dataNodeInZoneCId = null;
        for (Iterator<DiscoveryNode> it = dataNodes.valuesIt(); it.hasNext();) {
            DiscoveryNode node = it.next();
            switch (node.getAttributes().get("zone")) {
                case "a":
                    dataNodeInZoneAId = node.getId();
                    break;
                case "b":
                    dataNodeInZoneBId = node.getId();
                    break;
                case "c":
                    dataNodeInZoneCId = node.getId();
            }
        }

        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().execute().actionGet();
        for (NodeStats stat : nodeStats.getNodes()) {
            SearchStats.Stats searchStats = stat.getIndices().getSearch().getTotal();
            if (stat.getNode().isDataNode()) {
                if (stat.getNode().getId().equals(dataNodeInZoneBId)) {
                    Assert.assertTrue(searchStats.getQueryCount() > 0L);
                    Assert.assertTrue(searchStats.getFetchCount() > 0L);
                } else if (stat.getNode().getId().equals(dataNodeInZoneCId)) {
                    Assert.assertTrue(searchStats.getQueryCount() > 0L);
                }
            }
        }
        assertBusy(
            () -> assertThat(client().admin().indices().prepareStats().get().getTotal().getSearch().getOpenContexts(), equalTo(0L)),
            60,
            TimeUnit.SECONDS
        );
    }
}

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
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.PreferenceBasedSearchNotAllowedException;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.WeightedRouting;
import org.opensearch.cluster.routing.WeightedRoutingStats;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.search.stats.SearchStats;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.plugins.Plugin;
import org.opensearch.core.rest.RestStatus;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
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
            .put("cluster.routing.weighted.fail_open", false)
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
            .setVersion(-1)
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

        ClusterDeleteWeightedRoutingResponse deleteResponse = client().admin().cluster().prepareDeleteWeightedRouting().setVersion(0).get();
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

    private Map<String, List<String>> setupCluster(int nodeCountPerAZ, Settings commonSettings) {

        logger.info("--> starting a dedicated cluster manager node");
        internalCluster().startClusterManagerOnlyNode(Settings.builder().put(commonSettings).build());

        logger.info("--> starting 1 nodes on zones 'a' & 'b' & 'c'");
        Map<String, List<String>> nodeMap = new HashMap<>();
        List<String> nodes_in_zone_a = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build()
        );
        nodeMap.put("a", nodes_in_zone_a);
        List<String> nodes_in_zone_b = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build()
        );
        nodeMap.put("b", nodes_in_zone_b);

        List<String> nodes_in_zone_c = internalCluster().startDataOnlyNodes(
            nodeCountPerAZ,
            Settings.builder().put(commonSettings).put("node.attr.zone", "c").build()
        );
        nodeMap.put("c", nodes_in_zone_c);

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("4").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        ensureGreen();
        return nodeMap;

    }

    private void setUpIndexing(int numShards, int numReplicas) {
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder().put("index.number_of_shards", numShards).put("index.number_of_replicas", numReplicas)
            )
        );
        ensureGreen();

        logger.info("--> creating indices for test");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test").setId("" + i).setSource("field_" + i, "value_" + i).get();
        }
        refresh("test");
    }

    private void setShardRoutingWeights(Map<String, Double> weights) {
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

        ClusterPutWeightedRoutingResponse response = client().admin()
            .cluster()
            .prepareWeightedRouting()
            .setWeightedRouting(weightedRouting)
            .setVersion(-1)
            .get();
        assertEquals(response.isAcknowledged(), true);
    }

    /**
     * Shard routing request fail without fail-open if there are no healthy nodes in active az to serve request
     * This is tested by setting up a 3 node cluster with one data node per az.
     * Weighted shard routing weight is set as 0 for az-c.
     * Data nodes in zone a and b are stopped,
     * assertions are put to check that search requests fail.
     * @throws Exception throws Exception
     */
    public void testShardRoutingByStoppingDataNodes_FailOpenDisabled() throws Exception {

        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.weighted.fail_open", false)
            .build();

        int nodeCountPerAZ = 1;
        Map<String, List<String>> nodeMap = setupCluster(nodeCountPerAZ, commonSettings);

        int numShards = 1;
        int numReplicas = 2;
        setUpIndexing(numShards, numReplicas);

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        setShardRoutingWeights(weights);

        logger.info("--> data nodes in zone a and b are stopped");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeMap.get("a").get(0)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeMap.get("b").get(0)));
        ensureStableCluster(2);

        Set<String> hitNodes = new HashSet<>();

        // Make Search Requests
        Future<SearchResponse>[] responses = new Future[50];
        for (int i = 0; i < 50; i++) {
            responses[i] = internalCluster().smartClient().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).execute();
        }
        int failedCount = 0;
        for (int i = 0; i < 50; i++) {
            try {
                SearchResponse searchResponse = responses[i].get();
                assertEquals(0, searchResponse.getFailedShards());
                for (int j = 0; j < searchResponse.getHits().getHits().length; j++) {
                    hitNodes.add(searchResponse.getHits().getAt(j).getShard().getNodeId());
                }
            } catch (Exception t) {
                failedCount++;
            }
        }

        Assert.assertTrue(failedCount > 0);
        logger.info("--> failed request count is [()]", failedCount);
        assertNoSearchInAZ("c");
    }

    /**
     * Shard routing request with fail open enabled is served by data nodes in az with weight set as 0,
     * in case shard copies are not available in other azs (with fail open enabled)
     * This is tested by setting up a 3 node cluster with one data node per az.
     * Weighted shard routing weight is set as 0 for az-c.
     * Data nodes in zone a and b are stopped,
     * assertions are put to make sure shard search requests do not fail.
     * @throws IOException throws exception
     */
    public void testShardRoutingByStoppingDataNodes_FailOpenEnabled() throws Exception {

        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.weighted.fail_open", true)
            .build();

        int nodeCountPerAZ = 1;
        Map<String, List<String>> nodeMap = setupCluster(nodeCountPerAZ, commonSettings);

        int numShards = 1;
        int numReplicas = 2;
        setUpIndexing(numShards, numReplicas);

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        setShardRoutingWeights(weights);

        logger.info("--> data nodes in zone a and b are stopped");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeMap.get("a").get(0)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeMap.get("b").get(0)));
        ensureStableCluster(2);

        Set<String> hitNodes = new HashSet<>();

        // Make Search Requests
        Future<SearchResponse>[] responses = new Future[50];
        for (int i = 0; i < 50; i++) {
            responses[i] = internalCluster().smartClient().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).execute();
        }
        int failedCount = 0;
        for (int i = 0; i < 50; i++) {
            try {
                SearchResponse searchResponse = responses[i].get();
                assertEquals(0, searchResponse.getFailedShards());
                for (int j = 0; j < searchResponse.getHits().getHits().length; j++) {
                    hitNodes.add(searchResponse.getHits().getAt(j).getShard().getNodeId());
                }
            } catch (Exception t) {
                failedCount++;
            }
        }

        Assert.assertTrue(failedCount == 0);
        assertSearchInAZ("c");
    }

    /**
     * Shard routing request with fail open disabled is not served by data nodes in az with weight set as 0,
     * in case shard copies are not available in other azs.
     * This is tested by setting up a 3 node cluster with one data node per az.
     * Weighted shard routing weight is set as 0 for az-c.
     * Indices are created with one replica copy and network disruption is introduced,
     * which makes data node in zone-a unresponsive.
     * Since there are two copies of a shard, there can be few shards for which copy doesn't exist in zone b.
     * Assertions are put to make sure such shard search requests are not served by data node in zone c.
     * @throws IOException throws exception
     */
    public void testShardRoutingWithNetworkDisruption_FailOpenDisabled() throws Exception {

        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.weighted.fail_open", false)
            .build();

        int nodeCountPerAZ = 1;
        Map<String, List<String>> nodeMap = setupCluster(nodeCountPerAZ, commonSettings);

        int numShards = 10;
        int numReplicas = 1;
        setUpIndexing(numShards, numReplicas);

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        setShardRoutingWeights(weights);

        logger.info("--> creating network partition disruption");
        final String clusterManagerNode1 = internalCluster().getClusterManagerName();
        Set<String> nodesInOneSide = Stream.of(clusterManagerNode1, nodeMap.get("b").get(0), nodeMap.get("c").get(0))
            .collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInOtherSide = Stream.of(nodeMap.get("a").get(0)).collect(Collectors.toCollection(HashSet::new));

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
            responses[i] = internalCluster().client(nodeMap.get("b").get(0))
                .prepareSearch("test")
                .setQuery(QueryBuilders.matchAllQuery())
                .execute();
        }

        logger.info("--> network disruption is stopped");
        networkDisruption.stopDisrupting();

        int failedShardCount = 0;

        for (int i = 0; i < 50; i++) {
            try {
                SearchResponse searchResponse = responses[i].get();
                failedShardCount += searchResponse.getFailedShards();
                for (int j = 0; j < searchResponse.getHits().getHits().length; j++) {
                    hitNodes.add(searchResponse.getHits().getAt(j).getShard().getNodeId());
                }
            } catch (Exception t) {
                fail("search should not fail");
            }
        }
        Assert.assertTrue(failedShardCount > 0);
        // assert that no search request goes to az with weight zero
        assertNoSearchInAZ("c");
    }

    /**
     * Shard routing request is served by data nodes in az with weight set as 0,
     * in case shard copies are not available in other azs.(with fail open enabled)
     * This is tested by setting up a 3 node cluster with one data node per az.
     * Weighted shard routing weight is set as 0 for az-c.
     * Indices are created with one replica copy and network disruption is introduced,
     * which makes data node in zone-a unresponsive.
     * Since there are two copies of a shard, there can be few shards for which copy doesn't exist in zone b.
     * Assertions are put to make sure such shard search requests are served by data node in zone c.
     * @throws IOException throws exception
     */
    public void testShardRoutingWithNetworkDisruption_FailOpenEnabled() throws Exception {

        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.weighted.fail_open", true)
            .build();

        int nodeCountPerAZ = 1;
        Map<String, List<String>> nodeMap = setupCluster(nodeCountPerAZ, commonSettings);

        int numShards = 10;
        int numReplicas = 1;
        setUpIndexing(numShards, numReplicas);

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        setShardRoutingWeights(weights);

        logger.info("--> creating network partition disruption");
        final String clusterManagerNode1 = internalCluster().getClusterManagerName();
        Set<String> nodesInOneSide = Stream.of(clusterManagerNode1, nodeMap.get("b").get(0)).collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInOtherSide = Stream.of(nodeMap.get("a").get(0)).collect(Collectors.toCollection(HashSet::new));

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
            responses[i] = internalCluster().client(nodeMap.get("b").get(0))
                .prepareSearch("test")
                .setSize(100)
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

        assertSearchInAZ("b");
        assertSearchInAZ("c");
        assertNoSearchInAZ("a");
    }

    public void testStrictWeightedRoutingWithCustomString_FailOpenEnabled() throws Exception {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.weighted.fail_open", true)
            .put("cluster.routing.weighted.strict", true)
            .build();

        int nodeCountPerAZ = 1;
        Map<String, List<String>> nodeMap = setupCluster(nodeCountPerAZ, commonSettings);

        int numShards = 10;
        int numReplicas = 1;
        setUpIndexing(numShards, numReplicas);

        logger.info("--> creating network partition disruption");
        final String clusterManagerNode1 = internalCluster().getClusterManagerName();
        Set<String> nodesInOneSide = Stream.of(clusterManagerNode1, nodeMap.get("b").get(0)).collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInOtherSide = Stream.of(nodeMap.get("a").get(0)).collect(Collectors.toCollection(HashSet::new));

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        setShardRoutingWeights(weights);

        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(nodesInOneSide, nodesInOtherSide),
            NetworkDisruption.UNRESPONSIVE
        );
        internalCluster().setDisruptionScheme(networkDisruption);

        logger.info("--> network disruption is started");
        networkDisruption.startDisrupting();

        Set<String> hitNodes = new HashSet<>();
        Future<SearchResponse>[] responses = new Future[50];
        String customPreference = randomAlphaOfLength(10);
        logger.info("--> making search requests");
        for (int i = 0; i < 50; i++) {
            responses[i] = internalCluster().client(nodeMap.get("b").get(0))
                .prepareSearch("test")
                .setPreference(customPreference)
                .setSize(100)
                .setQuery(QueryBuilders.matchAllQuery())
                .execute();
        }

        logger.info("--> network disruption is stopped");
        networkDisruption.stopDisrupting();

        logger.info("--> shards should fail due to network disruption");
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

        try {
            assertSearchInAZ("b");
        } catch (AssertionError ae) {
            assertSearchInAZ("c");
        }
        assertNoSearchInAZ("a");
    }

    public void testStrictWeightedRoutingWithCustomString_FailOpenDisabled() throws Exception {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.weighted.fail_open", false)
            .put("cluster.routing.weighted.strict", true)
            .build();

        int nodeCountPerAZ = 1;
        Map<String, List<String>> nodeMap = setupCluster(nodeCountPerAZ, commonSettings);

        int numShards = 10;
        int numReplicas = 1;
        setUpIndexing(numShards, numReplicas);

        logger.info("--> creating network partition disruption");
        final String clusterManagerNode1 = internalCluster().getClusterManagerName();
        Set<String> nodesInOneSide = Stream.of(clusterManagerNode1, nodeMap.get("b").get(0)).collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInOtherSide = Stream.of(nodeMap.get("a").get(0)).collect(Collectors.toCollection(HashSet::new));

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        setShardRoutingWeights(weights);

        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(nodesInOneSide, nodesInOtherSide),
            NetworkDisruption.UNRESPONSIVE
        );
        internalCluster().setDisruptionScheme(networkDisruption);

        logger.info("--> network disruption is started");
        networkDisruption.startDisrupting();

        Set<String> hitNodes = new HashSet<>();
        Future<SearchResponse>[] responses = new Future[50];
        String customPreference = randomAlphaOfLength(10);
        logger.info("--> making search requests");
        for (int i = 0; i < 50; i++) {
            responses[i] = internalCluster().client(nodeMap.get("b").get(0))
                .prepareSearch("test")
                .setPreference(customPreference)
                .setSize(100)
                .setQuery(QueryBuilders.matchAllQuery())
                .execute();
        }

        logger.info("--> network disruption is stopped");
        networkDisruption.stopDisrupting();

        logger.info("--> shards should fail due to network disruption");
        for (int i = 0; i < 50; i++) {
            try {
                SearchResponse searchResponse = responses[i].get();
                assertNotEquals(searchResponse.getFailedShards(), 0);
                for (int j = 0; j < searchResponse.getHits().getHits().length; j++) {
                    hitNodes.add(searchResponse.getHits().getAt(j).getShard().getNodeId());
                }
            } catch (Exception t) {
                fail("search should not fail");
            }
        }

        DiscoveryNodes dataNodes = internalCluster().clusterService().state().nodes();
        Set<String> expectedHotNodes = new HashSet<>();
        for (DiscoveryNode node : dataNodes) {
            if (node.getAttributes().getOrDefault("zone", "").equals("b")) {
                expectedHotNodes.add(node.getId());
            }
        }

        assertEquals(expectedHotNodes, hitNodes);

        assertSearchInAZ("b");
        assertNoSearchInAZ("c");
        assertNoSearchInAZ("a");
    }

    /**
     * Should failopen shards even if failopen enabled with custom search preference.
     */
    public void testStrictWeightedRoutingWithShardPrefNetworkDisruption_FailOpenEnabled() throws Exception {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.weighted.fail_open", true)
            .put("cluster.routing.weighted.strict", true)
            .build();

        int nodeCountPerAZ = 1;
        Map<String, List<String>> nodeMap = setupCluster(nodeCountPerAZ, commonSettings);

        int numShards = 10;
        int numReplicas = 1;
        setUpIndexing(numShards, numReplicas);

        logger.info("--> creating network partition disruption");
        final String clusterManagerNode1 = internalCluster().getClusterManagerName();
        Set<String> nodesInOneSide = Stream.of(clusterManagerNode1, nodeMap.get("c").get(0)).collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInOtherSide = Stream.of(nodeMap.get("a").get(0)).collect(Collectors.toCollection(HashSet::new));

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        setShardRoutingWeights(weights);

        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(nodesInOneSide, nodesInOtherSide),
            NetworkDisruption.UNRESPONSIVE
        );
        internalCluster().setDisruptionScheme(networkDisruption);

        logger.info("--> network disruption is started");
        networkDisruption.startDisrupting();

        Future<SearchResponse>[] responses = new Future[50];
        DiscoveryNodes dataNodes = internalCluster().clusterService().state().nodes();
        ShardId shardId = internalCluster().clusterService()
            .state()
            .getRoutingTable()
            .index("test")
            .randomAllActiveShardsIt()
            .getShardRoutings()
            .stream()
            .filter(shard -> {
                return dataNodes.get(shard.currentNodeId()).getAttributes().getOrDefault("zone", "").equals("c");
            })
            .findFirst()
            .get()
            .shardId();

        for (int i = 0; i < 50; i++) {
            responses[i] = internalCluster().client(nodeMap.get("c").get(0))
                .prepareSearch("test")
                .setPreference(String.format(Locale.ROOT, "_shards:%s", shardId.getId()))
                .setSize(100)
                .setQuery(QueryBuilders.matchAllQuery())
                .execute();
        }

        logger.info("--> network disruption is stopped");
        networkDisruption.stopDisrupting();

        for (int i = 0; i < 50; i++) {
            try {
                SearchResponse searchResponse = responses[i].get();
                assertEquals(searchResponse.getFailedShards(), 0);
            } catch (Exception t) {
                fail("search should not fail");
            }
        }

        assertNoSearchInAZ("a");
        try {
            assertSearchInAZ("c");
        } catch (AssertionError ae) {
            assertSearchInAZ("b");
        }
    }

    public void testStrictWeightedRoutingWithShardPref() throws Exception {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.weighted.fail_open", true)
            .put("cluster.routing.weighted.strict", true)
            .build();

        int nodeCountPerAZ = 1;
        Map<String, List<String>> nodeMap = setupCluster(nodeCountPerAZ, commonSettings);

        int numShards = 10;
        int numReplicas = 1;
        setUpIndexing(numShards, numReplicas);

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        setShardRoutingWeights(weights);

        DiscoveryNodes dataNodes = internalCluster().clusterService().state().nodes();
        ShardId shardId = internalCluster().clusterService()
            .state()
            .getRoutingTable()
            .index("test")
            .randomAllActiveShardsIt()
            .getShardRoutings()
            .stream()
            .filter(shard -> {
                return dataNodes.get(shard.currentNodeId()).getAttributes().getOrDefault("zone", "").equals("c");
            })
            .findFirst()
            .get()
            .shardId();

        Future<SearchResponse>[] responses = new Future[50];
        logger.info("--> making search requests");
        for (int i = 0; i < 50; i++) {
            responses[i] = internalCluster().client(nodeMap.get("b").get(0))
                .prepareSearch("test")
                .setPreference(String.format(Locale.ROOT, "_shards:%s", shardId.getId()))
                .setSize(100)
                .setQuery(QueryBuilders.matchAllQuery())
                .execute();
        }

        for (int i = 0; i < 50; i++) {
            try {
                SearchResponse searchResponse = responses[i].get();
                assertEquals(searchResponse.getFailedShards(), 0);
                assertNotEquals(searchResponse.getHits().getTotalHits().value, 0);
            } catch (Exception t) {
                fail("search should not fail");
            }
        }
        assertNoSearchInAZ("c");
    }

    private void assertNoSearchInAZ(String az) {
        final Map<String, DiscoveryNode> dataNodes = internalCluster().clusterService().state().nodes().getDataNodes();
        String dataNodeId = null;

        for (Iterator<DiscoveryNode> it = dataNodes.values().iterator(); it.hasNext();) {
            DiscoveryNode node = it.next();
            if (node.getAttributes().get("zone").equals(az)) {
                dataNodeId = node.getId();
                break;
            }
        }

        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().execute().actionGet();
        for (NodeStats stat : nodeStats.getNodes()) {
            SearchStats.Stats searchStats = stat.getIndices().getSearch().getTotal();
            if (stat.getNode().isDataNode()) {
                if (stat.getNode().getId().equals(dataNodeId)) {
                    assertEquals(0, searchStats.getQueryCount());
                    assertEquals(0, searchStats.getFetchCount());
                }
            }
        }
    }

    private void assertSearchInAZ(String az) {
        final Map<String, DiscoveryNode> dataNodes = internalCluster().clusterService().state().nodes().getDataNodes();
        String dataNodeId = null;

        for (Iterator<DiscoveryNode> it = dataNodes.values().iterator(); it.hasNext();) {
            DiscoveryNode node = it.next();
            if (node.getAttributes().get("zone").equals(az)) {
                dataNodeId = node.getId();
                break;
            }
        }

        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().execute().actionGet();
        for (NodeStats stat : nodeStats.getNodes()) {
            SearchStats.Stats searchStats = stat.getIndices().getSearch().getTotal();
            if (stat.getNode().isDataNode()) {
                if (stat.getNode().getId().equals(dataNodeId)) {
                    Assert.assertTrue(searchStats.getFetchCount() > 0L);
                    Assert.assertTrue(searchStats.getQueryCount() > 0L);
                }
            }
        }
    }

    /**
     * Shard routing request is served by data nodes in az with weight set as 0,
     * in case shard copies are not available in other azs.
     * This is tested by setting up a 3 node cluster with one data node per az.
     * Weighted shard routing weight is set as 0 for az-c.
     * Indices are created with one replica copy and network disruption is introduced,
     * which makes node in zone-a unresponsive.
     * Since there are two copies of a shard, there can be few shards for which copy doesn't exist in zone b.
     * Assertions are put to make sure such shard search requests are served by data node in zone c.
     * @throws IOException throws exception
     */
    public void testSearchAggregationWithNetworkDisruption_FailOpenEnabled() throws Exception {

        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.weighted.fail_open", true)
            .build();

        int nodeCountPerAZ = 1;
        Map<String, List<String>> nodeMap = setupCluster(nodeCountPerAZ, commonSettings);

        assertAcked(
            prepareCreate("index").setMapping("f", "type=keyword")
                .setSettings(Settings.builder().put("index" + ".number_of_shards", 10).put("index" + ".number_of_replicas", 1))
        );

        int numDocs = 10;
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; ++i) {
            docs.add(client().prepareIndex("index").setSource("f", Integer.toString(i / 3)));
        }
        indexRandom(true, docs);
        ensureGreen();
        refresh("index");

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        setShardRoutingWeights(weights);

        logger.info("--> creating network partition disruption");
        final String clusterManagerNode1 = internalCluster().getClusterManagerName();
        Set<String> nodesInOneSide = Stream.of(clusterManagerNode1, nodeMap.get("b").get(0), nodeMap.get("c").get(0))
            .collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInOtherSide = Stream.of(nodeMap.get("a").get(0)).collect(Collectors.toCollection(HashSet::new));

        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(nodesInOneSide, nodesInOtherSide),
            NetworkDisruption.UNRESPONSIVE
        );
        internalCluster().setDisruptionScheme(networkDisruption);

        logger.info("--> network disruption is started");
        networkDisruption.startDisrupting();

        Set<String> hitNodes = new HashSet<>();
        Future<SearchResponse>[] responses = new Future[51];
        int size = 17;
        logger.info("--> making search requests");
        for (int i = 0; i < 50; i++) {
            responses[i] = internalCluster().client(nodeMap.get("b").get(0))
                .prepareSearch("index")
                .setSize(20)
                .addAggregation(terms("f").field("f"))
                .execute();
        }

        logger.info("--> network disruption is stopped");
        networkDisruption.stopDisrupting();

        for (int i = 0; i < 50; i++) {
            try {
                SearchResponse searchResponse = responses[i].get();
                Aggregations aggregations = searchResponse.getAggregations();
                assertNotNull(aggregations);
                Terms terms = aggregations.get("f");
                assertEquals(0, searchResponse.getFailedShards());
                assertEquals(Math.min(numDocs, 3L), terms.getBucketByKey("0").getDocCount());
                for (int j = 0; j < searchResponse.getHits().getHits().length; j++) {
                    hitNodes.add(searchResponse.getHits().getAt(j).getShard().getNodeId());
                }
            } catch (Exception t) {
                fail("search should not fail");
            }
        }
        assertSearchInAZ("b");
        assertSearchInAZ("c");
        assertNoSearchInAZ("a");

        assertBusy(
            () -> assertThat(client().admin().indices().prepareStats().get().getTotal().getSearch().getOpenContexts(), equalTo(0L)),
            60,
            TimeUnit.SECONDS
        );
    }

    /**
     * MultiGet with fail open enabled. No request failure on network disruption
     * @throws IOException throws exception
     */
    public void testMultiGetWithNetworkDisruption_FailOpenEnabled() throws Exception {

        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.weighted.fail_open", true)
            .build();

        int nodeCountPerAZ = 1;
        Map<String, List<String>> nodeMap = setupCluster(nodeCountPerAZ, commonSettings);

        int numShards = 10;
        int numReplicas = 1;
        setUpIndexing(numShards, numReplicas);

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        setShardRoutingWeights(weights);

        logger.info("--> creating network partition disruption");
        final String clusterManagerNode1 = internalCluster().getClusterManagerName();
        Set<String> nodesInOneSide = Stream.of(clusterManagerNode1, nodeMap.get("b").get(0), nodeMap.get("c").get(0))
            .collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInOtherSide = Stream.of(nodeMap.get("a").get(0)).collect(Collectors.toCollection(HashSet::new));

        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(nodesInOneSide, nodesInOtherSide),
            NetworkDisruption.UNRESPONSIVE
        );
        internalCluster().setDisruptionScheme(networkDisruption);

        logger.info("--> network disruption is started");
        networkDisruption.startDisrupting();

        Future<MultiGetResponse>[] responses = new Future[50];
        logger.info("--> making search requests");
        int index1, index2;
        for (int i = 0; i < 50; i++) {
            index1 = randomIntBetween(0, 9);
            index2 = randomIntBetween(0, 9);
            responses[i] = client().prepareMultiGet()
                .add(new MultiGetRequest.Item("test", "" + index1))
                .add(new MultiGetRequest.Item("test", "" + index2))
                .execute();
        }

        logger.info("--> network disruption is stopped");
        networkDisruption.stopDisrupting();

        for (int i = 0; i < 50; i++) {
            try {
                MultiGetResponse multiGetResponse = responses[i].get();
                assertThat(multiGetResponse.getResponses().length, equalTo(2));
                assertThat(multiGetResponse.getResponses()[0].isFailed(), equalTo(false));
                assertThat(multiGetResponse.getResponses()[1].isFailed(), equalTo(false));
            } catch (Exception t) {
                fail("search should not fail");
            }
        }

        assertBusy(
            () -> assertThat(client().admin().indices().prepareStats().get().getTotal().getSearch().getOpenContexts(), equalTo(0L)),
            60,
            TimeUnit.SECONDS
        );
    }

    /**
     * MultiGet with fail open disabled. Assert that some requests do fail.
     * @throws IOException throws exception
     */
    public void testMultiGetWithNetworkDisruption_FailOpenDisabled() throws Exception {

        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.weighted.fail_open", false)
            .build();

        int nodeCountPerAZ = 1;
        Map<String, List<String>> nodeMap = setupCluster(nodeCountPerAZ, commonSettings);

        int numShards = 10;
        int numReplicas = 1;
        setUpIndexing(numShards, numReplicas);

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        setShardRoutingWeights(weights);

        logger.info("--> creating network partition disruption");
        final String clusterManagerNode1 = internalCluster().getClusterManagerName();
        Set<String> nodesInOneSide = Stream.of(clusterManagerNode1, nodeMap.get("b").get(0), nodeMap.get("c").get(0))
            .collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInOtherSide = Stream.of(nodeMap.get("a").get(0)).collect(Collectors.toCollection(HashSet::new));

        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(nodesInOneSide, nodesInOtherSide),
            NetworkDisruption.UNRESPONSIVE
        );
        internalCluster().setDisruptionScheme(networkDisruption);

        logger.info("--> network disruption is started");
        networkDisruption.startDisrupting();

        Future<MultiGetResponse>[] responses = new Future[50];
        logger.info("--> making search requests");
        int index1, index2;
        for (int i = 0; i < 50; i++) {
            index1 = randomIntBetween(0, 9);
            index2 = randomIntBetween(0, 9);
            responses[i] = client().prepareMultiGet()
                .add(new MultiGetRequest.Item("test", "" + index1))
                .add(new MultiGetRequest.Item("test", "" + index2))
                .execute();
        }

        logger.info("--> network disruption is stopped");
        networkDisruption.stopDisrupting();
        int failedCount = 0;
        for (int i = 0; i < 50; i++) {
            try {
                MultiGetResponse multiGetResponse = responses[i].get();
                assertThat(multiGetResponse.getResponses().length, equalTo(2));
                if (multiGetResponse.getResponses()[0].isFailed() || multiGetResponse.getResponses()[1].isFailed()) {
                    failedCount++;
                }
            } catch (Exception t) {
                fail("search should not fail");
            }
        }

        Assert.assertTrue(failedCount > 0);

        assertBusy(
            () -> assertThat(client().admin().indices().prepareStats().get().getTotal().getSearch().getOpenContexts(), equalTo(0L)),
            60,
            TimeUnit.SECONDS
        );
    }

    /**
     * Assert that preference search with custom string doesn't hit a node in weighed away az
     */
    public void testStrictWeightedRoutingWithCustomString() {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.weighted.fail_open", true)
            .put("cluster.routing.weighted.strict", true)
            .build();

        int nodeCountPerAZ = 1;
        Map<String, List<String>> nodeMap = setupCluster(nodeCountPerAZ, commonSettings);

        int numShards = 20;
        int numReplicas = 2;
        setUpIndexing(numShards, numReplicas);

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        setShardRoutingWeights(weights);
        String customPreference = randomAlphaOfLength(10);

        SearchResponse searchResponse = internalCluster().client(nodeMap.get("b").get(0))
            .prepareSearch()
            .setSize(20)
            .setPreference(customPreference)
            .get();
        assertEquals(RestStatus.OK.getStatus(), searchResponse.status().getStatus());
        assertNoSearchInAZ("c");
        assertSearchInAZ("a");
        assertSearchInAZ("b");

        // disable strict weighed routing
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("cluster.routing.weighted.strict", false))
            .get();

        // make search requests with custom string
        internalCluster().client(nodeMap.get("a").get(0))
            .prepareSearch()
            .setSize(20)
            .setPreference(customPreference)
            .setQuery(QueryBuilders.matchAllQuery())
            .get();
        // assert search on data nodes on az c (weighed away az)
        try {
            assertSearchInAZ("c");
        } catch (AssertionError ae) {
            assertSearchInAZ("a");
        }

    }

    /**
     *  Assert that preference based search works with non-strict weighted shard routing
     */
    public void testPreferenceSearchWithWeightedRouting() {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.weighted.fail_open", true)
            .put("cluster.routing.weighted.strict", false)
            .build();

        int nodeCountPerAZ = 1;
        Map<String, List<String>> nodeMap = setupCluster(nodeCountPerAZ, commonSettings);

        int numShards = 10;
        int numReplicas = 2;
        setUpIndexing(numShards, numReplicas);

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        setShardRoutingWeights(weights);

        String customPreference = randomAlphaOfLength(10);
        String nodeInZoneA = nodeMap.get("a").get(0);
        String nodeInZoneB = nodeMap.get("b").get(0);
        String nodeInZoneC = nodeMap.get("c").get(0);

        Map<String, String> nodeIDMap = new HashMap<>();
        DiscoveryNodes dataNodes = internalCluster().clusterService().state().nodes();
        for (DiscoveryNode node : dataNodes) {
            nodeIDMap.put(node.getName(), node.getId());
        }

        SearchResponse searchResponse = internalCluster().client(nodeMap.get("b").get(0))
            .prepareSearch()
            .setPreference(randomFrom("_local", "_prefer_nodes:" + "zone:a", customPreference))
            .get();
        assertEquals(RestStatus.OK.getStatus(), searchResponse.status().getStatus());

        searchResponse = internalCluster().client(nodeMap.get("a").get(0))
            .prepareSearch()
            .setPreference(
                "_only_nodes:" + nodeIDMap.get(nodeInZoneA) + "," + nodeIDMap.get(nodeInZoneB) + "," + nodeIDMap.get(nodeInZoneC)
            )
            .get();
        assertEquals(RestStatus.OK.getStatus(), searchResponse.status().getStatus());
    }

    public void testPreferenceSearchWithIgnoreWeightedRouting() {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.weighted.fail_open", true)
            .put("cluster.routing.weighted.strict", false)
            .put("cluster.routing.ignore_weighted_routing", true)
            .build();

        int nodeCountPerAZ = 1;
        Map<String, List<String>> nodeMap = setupCluster(nodeCountPerAZ, commonSettings);

        int numShards = 10;
        int numReplicas = 2;
        setUpIndexing(numShards, numReplicas);

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        setShardRoutingWeights(weights);

        String customPreference = randomAlphaOfLength(10);
        String nodeInZoneA = nodeMap.get("a").get(0);
        String nodeInZoneB = nodeMap.get("b").get(0);
        String nodeInZoneC = nodeMap.get("c").get(0);

        Map<String, String> nodeIDMap = new HashMap<>();
        DiscoveryNodes dataNodes = internalCluster().clusterService().state().nodes();
        for (DiscoveryNode node : dataNodes) {
            nodeIDMap.put(node.getName(), node.getId());
        }

        SearchResponse searchResponse = internalCluster().client(nodeMap.get("b").get(0))
            .prepareSearch()
            .setPreference(randomFrom("_local", "_prefer_nodes:" + "zone:a", customPreference))
            .get();
        assertEquals(RestStatus.OK.getStatus(), searchResponse.status().getStatus());

        searchResponse = internalCluster().client(nodeMap.get("a").get(0))
            .prepareSearch()
            .setPreference(
                "_only_nodes:" + nodeIDMap.get(nodeInZoneA) + "," + nodeIDMap.get(nodeInZoneB) + "," + nodeIDMap.get(nodeInZoneC)
            )
            .get();
        assertEquals(RestStatus.OK.getStatus(), searchResponse.status().getStatus());
    }

    /**
     * Assert that preference based search with preference type is not allowed with strict weighted shard routing
     */
    public void testStrictWeightedRouting() {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.weighted.fail_open", true)
            .put("cluster.routing.weighted.strict", true)
            .build();

        int nodeCountPerAZ = 1;
        Map<String, List<String>> nodeMap = setupCluster(nodeCountPerAZ, commonSettings);

        int numShards = 10;
        int numReplicas = 1;
        setUpIndexing(numShards, numReplicas);

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        setShardRoutingWeights(weights);
        String nodeInZoneA = nodeMap.get("a").get(0);

        assertThrows(
            PreferenceBasedSearchNotAllowedException.class,
            () -> internalCluster().client(nodeMap.get("b").get(0))
                .prepareSearch()
                .setSize(0)
                .setPreference("_only_nodes:" + nodeInZoneA)
                .get()
        );

        assertThrows(
            PreferenceBasedSearchNotAllowedException.class,
            () -> internalCluster().client(nodeMap.get("b").get(0))
                .prepareSearch()
                .setSize(0)
                .setPreference("_prefer_nodes:" + nodeInZoneA)
                .get()
        );
    }

    public void testStrictWeightedRoutingAllowedForSomeSearchPrefs() {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.weighted.fail_open", true)
            .put("cluster.routing.weighted.strict", true)
            .build();

        int nodeCountPerAZ = 1;
        Map<String, List<String>> nodeMap = setupCluster(nodeCountPerAZ, commonSettings);

        int numShards = 10;
        int numReplicas = 1;
        setUpIndexing(numShards, numReplicas);

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        setShardRoutingWeights(weights);
        String nodeInZoneA = nodeMap.get("a").get(0);
        String customPreference = randomAlphaOfLength(10);

        SearchResponse searchResponse = internalCluster().client(nodeMap.get("b").get(0))
            .prepareSearch()
            .setSize(0)
            .setPreference("_only_local:" + nodeInZoneA)
            .get();
        assertEquals(RestStatus.OK.getStatus(), searchResponse.status().getStatus());

        searchResponse = internalCluster().client(nodeMap.get("b").get(0))
            .prepareSearch()
            .setSize(0)
            .setPreference("_local:" + nodeInZoneA)
            .get();
        assertEquals(RestStatus.OK.getStatus(), searchResponse.status().getStatus());

        searchResponse = internalCluster().client(nodeMap.get("b").get(0)).prepareSearch().setSize(0).setPreference("_shards:1").get();
        assertEquals(RestStatus.OK.getStatus(), searchResponse.status().getStatus());

        searchResponse = internalCluster().client(nodeMap.get("b").get(0)).prepareSearch().setSize(0).setPreference(customPreference).get();
        assertEquals(RestStatus.OK.getStatus(), searchResponse.status().getStatus());
    }

    /**
     * Shard routing request is served by data nodes in az with weight set as 0,
     * in case shard copies are not available in other azs.(with fail open enabled)
     * This is tested by setting up a 3 node cluster with one data node per az.
     * Weighted shard routing weight is set as 0 for az-c.
     * Indices are created with one replica copy and network disruption is introduced,
     * which makes data node in zone-a unresponsive.
     * Since there are two copies of a shard, there can be few shards for which copy doesn't exist in zone b.
     * Assertions are put to make sure such shard search requests are served by data node in zone c.
     * Asserts on fail open stats which captures number of times fail open is executed
     * @throws IOException throws exception
     */
    public void testWeightedRoutingFailOpenStats() throws Exception {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.weighted.fail_open", true)
            .build();

        int nodeCountPerAZ = 1;
        Map<String, List<String>> nodeMap = setupCluster(nodeCountPerAZ, commonSettings);

        int numShards = 10;
        int numReplicas = 1;
        setUpIndexing(numShards, numReplicas);

        logger.info("--> setting shard routing weights for weighted round robin");
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        setShardRoutingWeights(weights);
        WeightedRoutingStats.getInstance().resetFailOpenCount();

        logger.info("--> creating network partition disruption");
        final String clusterManagerNode1 = internalCluster().getClusterManagerName();
        Set<String> nodesInOneSide = Stream.of(clusterManagerNode1, nodeMap.get("b").get(0)).collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInOtherSide = Stream.of(nodeMap.get("a").get(0)).collect(Collectors.toCollection(HashSet::new));

        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(nodesInOneSide, nodesInOtherSide),
            NetworkDisruption.UNRESPONSIVE
        );
        internalCluster().setDisruptionScheme(networkDisruption);

        DiscoveryNodes dataNodes = internalCluster().clusterService().state().nodes();

        Map<String, String> nodeIDMap = new HashMap<>();
        for (DiscoveryNode node : dataNodes) {
            nodeIDMap.put(node.getName(), node.getId());
        }

        List<ShardRouting> shardInNodeA = internalCluster().clusterService()
            .state()
            .getRoutingNodes()
            .node(nodeIDMap.get(nodeMap.get("a").get(0)))
            .shardsWithState(ShardRoutingState.STARTED);

        List<ShardRouting> shardInNodeC = internalCluster().clusterService()
            .state()
            .getRoutingNodes()
            .node(nodeIDMap.get(nodeMap.get("c").get(0)))
            .shardsWithState(ShardRoutingState.STARTED);

        // fail open will be called for shards in zone-a data node with replica in zone-c data node
        Set<ShardId> result = new HashSet<>();
        int failOpenShardCount = 0;
        for (ShardRouting shardRouting : shardInNodeA) {
            result.add(shardRouting.shardId());
        }
        for (ShardRouting shardRouting : shardInNodeC) {
            if (result.contains(shardRouting.shardId())) {
                failOpenShardCount++;
            }
        }

        logger.info("--> network disruption is started");
        networkDisruption.startDisrupting();

        Set<String> hitNodes = new HashSet<>();
        logger.info("--> making search requests");

        Future<SearchResponse> response = internalCluster().client(nodeMap.get("b").get(0))
            .prepareSearch("test")
            .setSize(100)
            .setQuery(QueryBuilders.matchAllQuery())
            .execute();

        logger.info("--> network disruption is stopped");
        networkDisruption.stopDisrupting();

        try {
            SearchResponse searchResponse = response.get();
            assertEquals(searchResponse.getFailedShards(), 0);
            for (int j = 0; j < searchResponse.getHits().getHits().length; j++) {
                hitNodes.add(searchResponse.getHits().getAt(j).getShard().getNodeId());
            }
        } catch (Exception t) {
            fail("search should not fail");
        }
        assertSearchInAZ("b");
        assertSearchInAZ("c");
        assertNoSearchInAZ("a");

        NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats().addMetric("weighted_routing").execute().actionGet();
        Map<String, NodeStats> stats = nodeStats.getNodesMap();
        NodeStats nodeStatsC = stats.get(nodeIDMap.get(nodeMap.get("c").get(0)));
        assertEquals(failOpenShardCount, nodeStatsC.getWeightedRoutingStats().getFailOpenCount());
        WeightedRoutingStats.getInstance().resetFailOpenCount();
    }

}

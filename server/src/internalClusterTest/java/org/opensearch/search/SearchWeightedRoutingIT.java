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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.WeightedRouting;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.search.stats.SearchStats;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, minNumDataNodes = 3)
public class SearchWeightedRoutingIT extends OpenSearchIntegTestCase {
    @Override
    protected int numberOfReplicas() {
        return 2;
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
        // search should not go to nodes in zone c
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

}

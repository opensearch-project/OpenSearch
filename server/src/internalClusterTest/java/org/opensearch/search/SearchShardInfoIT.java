/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchShardInfo;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * End-to-end coverage for the opt-in {@code shard_info} section of search responses: entries must
 * describe real shard copies on the nodes that actually served the request, skipped shards must
 * carry no node attribution, failed shards must be attributed to the copy that failed, and the
 * array sizes must reconcile with the {@code _shards} counters.
 */
public class SearchShardInfoIT extends OpenSearchIntegTestCase {

    public void testShardInfoMatchesClusterState() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 1))
        );
        ensureGreen("test");
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("value", i).get();
        }
        refresh("test");

        // captured before the search so that a relocation afterwards cannot make the cross-check spuriously fail
        ClusterState state = client().admin().cluster().prepareState().get().getState();

        SearchRequest request = new SearchRequest("test");
        request.shardInfo(true);
        request.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        SearchResponse response = client().search(request).actionGet();

        SearchShardInfo shardInfo = response.getShardInfo();
        assertNotNull(shardInfo);
        assertEquals(response.getSuccessfulShards(), shardInfo.getSuccessful().size() + shardInfo.getSkipped().size());
        assertEquals(response.getFailedShards(), shardInfo.getFailed().size());
        assertEquals(3, shardInfo.getSuccessful().size());
        assertTrue(shardInfo.getSkipped().isEmpty());
        assertTrue(shardInfo.getFailed().isEmpty());

        for (SearchShardInfo.Entry entry : shardInfo.getSuccessful()) {
            assertEquals("test", entry.getIndex());
            assertNotNull(entry.getNodeId());
            assertNotNull(entry.getPrimary());
            assertEquals("STARTED", entry.getState());
            assertNull("local searches must not carry a cluster alias", entry.getCluster());

            // the reported copy must exist in the routing table on exactly the reported node
            List<ShardRouting> copies = state.routingTable().index("test").shard(entry.getShard()).shards();
            ShardRouting matching = copies.stream()
                .filter(routing -> entry.getNodeId().equals(routing.currentNodeId()))
                .findFirst()
                .orElse(null);
            assertNotNull("entry must correspond to a real shard copy: " + entry, matching);
            assertEquals(matching.primary(), entry.getPrimary().booleanValue());

            DiscoveryNode node = state.nodes().get(entry.getNodeId());
            assertNotNull("entry must name a node that was in the cluster: " + entry, node);
            assertEquals(node.getName(), entry.getNodeName());
        }
    }

    public void testShardInfoListsCanMatchSkippedShardsWithoutNodeAttribution() {
        assertAcked(
            prepareCreate("skip_a").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping("created_at", "type=date,format=strict_date")
        );
        assertAcked(
            prepareCreate("skip_b").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping("created_at", "type=date,format=strict_date")
        );
        client().prepareIndex("skip_a").setId("1").setSource("created_at", "2016-01-01").get();
        client().prepareIndex("skip_b").setId("2").setSource("created_at", "2017-01-01").get();
        refresh("skip_a", "skip_b");

        SearchRequest request = new SearchRequest("skip_a", "skip_b");
        request.shardInfo(true);
        request.setPreFilterShardSize(1);
        request.source(new SearchSourceBuilder().size(0).query(QueryBuilders.rangeQuery("created_at").gte("2016-02-01").lt("2017-02-01")));
        SearchResponse response = client().search(request).actionGet();

        assertEquals(2, response.getTotalShards());
        assertEquals(2, response.getSuccessfulShards());
        assertEquals(1, response.getSkippedShards());
        assertEquals(0, response.getFailedShards());

        SearchShardInfo shardInfo = response.getShardInfo();
        assertNotNull(shardInfo);
        assertEquals(response.getSuccessfulShards(), shardInfo.getSuccessful().size() + shardInfo.getSkipped().size());
        assertEquals(1, shardInfo.getSuccessful().size());
        assertEquals("skip_b", shardInfo.getSuccessful().get(0).getIndex());
        assertEquals(1, shardInfo.getSkipped().size());

        SearchShardInfo.Entry skipped = shardInfo.getSkipped().get(0);
        assertEquals("skip_a", skipped.getIndex());
        assertEquals(0, skipped.getShard());
        assertNull("no node executed anything for a skipped shard", skipped.getNodeId());
        assertNull(skipped.getNodeName());
        assertNull(skipped.getPrimary());
        assertNull(skipped.getState());
    }

    public void testShardInfoAttributesFailedShards() {
        assertAcked(
            prepareCreate("good").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping("sortfield", "type=long")
        );
        assertAcked(
            prepareCreate("bad").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping("sortfield", "type=text")
        );
        client().prepareIndex("good").setId("1").setSource("sortfield", 1).get();
        client().prepareIndex("bad").setId("2").setSource("sortfield", "one").get();
        refresh("good", "bad");

        // sorting on a text field fails on the shards of "bad" only
        SearchRequest request = new SearchRequest("good", "bad");
        request.shardInfo(true);
        request.allowPartialSearchResults(true);
        request.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).sort(SortBuilders.fieldSort("sortfield")));
        SearchResponse response = client().search(request).actionGet();

        assertEquals(2, response.getTotalShards());
        assertEquals(1, response.getSuccessfulShards());
        assertEquals(1, response.getFailedShards());

        SearchShardInfo shardInfo = response.getShardInfo();
        assertNotNull(shardInfo);
        assertEquals(response.getSuccessfulShards(), shardInfo.getSuccessful().size() + shardInfo.getSkipped().size());
        assertEquals(response.getFailedShards(), shardInfo.getFailed().size());

        assertEquals(1, shardInfo.getSuccessful().size());
        assertEquals("good", shardInfo.getSuccessful().get(0).getIndex());

        SearchShardInfo.Entry failed = shardInfo.getFailed().get(0);
        assertEquals("bad", failed.getIndex());
        assertEquals(0, failed.getShard());
        assertNotNull("the failure must be attributed to the node that served the shard", failed.getNodeId());
        assertEquals(Boolean.TRUE, failed.getPrimary());
        assertEquals("STARTED", failed.getState());
    }
}

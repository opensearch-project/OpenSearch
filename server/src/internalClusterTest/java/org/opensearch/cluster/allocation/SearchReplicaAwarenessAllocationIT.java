/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AwarenessReplicaBalance;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.test.NodeRoles.searchOnlyNode;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchReplicaAwarenessAllocationIT extends RemoteStoreBaseIntegTestCase {

    private final Logger logger = LogManager.getLogger(SearchReplicaAwarenessAllocationIT.class);

    public void testAllocationAwarenessZones() {
        Settings commonSettings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values", "a,b")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .build();

        logger.info("--> starting 8 nodes on different zones");
        List<String> nodes = internalCluster().startNodes(
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").put(searchOnlyNode()).build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").put(searchOnlyNode()).build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").put(searchOnlyNode()).build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").put(searchOnlyNode()).build()
        );

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("6").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> create index");
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 2)
                .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()
        );

        logger.info("--> waiting for shards to be allocated");
        ensureGreen("test");

        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        final Map<String, Integer> counts = new HashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.merge(clusterState.nodes().get(shardRouting.currentNodeId()).getName(), 1, Integer::sum);
                }
            }
        }

        /*
         * Ensures that shards are distributed across different zones in the cluster.
         * Given two zones (a and b) with one data node in each, the shards are evenly distributed,
         * resulting in each data node being assigned three shards.
         */
        for (int i = 0; i < 2; i++) {
            assertThat(counts.get(nodes.get(i)), equalTo(3));
        }

        /*
         * There are two search nodes in each zone, totaling four search nodes.
         * With six search shards to allocate, they are assigned using a best-effort spread,
         * ensuring each search node receives either one or two shards.
         */
        for (int i = 2; i < 6; i++) {
            assertThat(counts.get(nodes.get(i)), anyOf(equalTo(1), equalTo(2)));
        }
    }

    public void testAwarenessZonesIncrementalNodes() {
        Settings commonSettings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values", "a,b")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .build();

        logger.info("--> starting 2 nodes on zones 'a' & 'b'");
        List<String> nodes = internalCluster().startNodes(
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").put(searchOnlyNode()).build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").put(searchOnlyNode()).build()
        );

        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 2)
                .build()
        );

        ensureGreen("test");

        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        Map<String, Integer> counts = new HashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.merge(clusterState.nodes().get(shardRouting.currentNodeId()).getName(), 1, Integer::sum);
                }
            }
        }

        /*
         * The cluster consists of two zones, each containing one data node and one search node.
         * Replicas and search replicas are evenly distributed across these zones.
         */
        for (int i = 0; i < 4; i++) {
            assertThat(counts.get(nodes.get(i)), equalTo(3));
        }

        logger.info("--> starting another data and search node in zone 'b'");

        String B_2 = internalCluster().startNode(Settings.builder().put(commonSettings).put("node.attr.zone", "b").build());
        String B_3 = internalCluster().startNode(
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").put(searchOnlyNode()).build()
        );

        ensureGreen("test");

        client().admin().cluster().prepareReroute().get();

        ensureGreen("test");

        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();

        counts = new HashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.merge(clusterState.nodes().get(shardRouting.currentNodeId()).getName(), 1, Integer::sum);
                }
            }
        }

        /*
         * Adding a new data node and a new search node in zone B results in:
         * - Zone A: 1 data node, 1 search node
         * - Zone B: 2 data nodes, 2 search nodes
         *
         * As a result, shards are rerouted to maintain a best-effort balanced allocation.
         */
        assertThat(counts.get(nodes.get(0)), equalTo(3));
        assertThat(counts.get(nodes.get(1)), equalTo(2));
        assertThat(counts.get(nodes.get(2)), equalTo(3));
        assertThat(counts.get(nodes.get(3)), equalTo(2));
        assertThat(counts.get(B_2), equalTo(1));
        assertThat(counts.get(B_3), equalTo(1));

        logger.info("--> starting another data node without any zone");

        String noZoneNode = internalCluster().startNode();
        ensureGreen("test");
        client().admin().cluster().prepareReroute().get();
        ensureGreen("test");
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();

        counts = new HashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.merge(clusterState.nodes().get(shardRouting.currentNodeId()).getName(), 1, Integer::sum);
                }
            }
        }

        logger.info("--> Ensure there was not rerouting");

        /*
         * Adding another node to the cluster without a zone attribute
         * does not trigger shard reallocation; existing shard assignments remain unchanged.
         */
        assertThat(counts.get(nodes.get(0)), equalTo(3));
        assertThat(counts.get(nodes.get(1)), equalTo(2));
        assertThat(counts.get(nodes.get(2)), equalTo(3));
        assertThat(counts.get(nodes.get(3)), equalTo(2));
        assertThat(counts.get(B_2), equalTo(1));
        assertThat(counts.get(B_3), equalTo(1));
        assertThat(counts.containsKey(noZoneNode), equalTo(false));

        logger.info("--> Remove the awareness attribute setting");

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "")
                    .build()
            )
            .get();

        ensureGreen("test");
        clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();

        counts = new HashMap<>();

        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.merge(clusterState.nodes().get(shardRouting.currentNodeId()).getName(), 1, Integer::sum);
                }
            }
        }

        /*
         * Removing allocation awareness attributes from the cluster disables zone-based distribution.
         * Shards are then assigned based solely the other deciders in the cluster manager.
         */
        assertThat(counts.get(nodes.get(0)), equalTo(2));
        assertThat(counts.get(nodes.get(1)), equalTo(2));
        assertThat(counts.get(nodes.get(2)), equalTo(2));
        assertThat(counts.get(nodes.get(3)), equalTo(2));
        assertThat(counts.get(B_2), equalTo(1));
        assertThat(counts.get(B_3), equalTo(2));
        assertThat(counts.get(noZoneNode), equalTo(1));
    }

    public void testAwarenessBalanceWithForcedAwarenessCreateIndex() {
        Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values", "a,b,c")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), "true")
            .build();

        logger.info("--> starting 3 nodes on zones a,b,c");
        internalCluster().startNodes(
            Settings.builder().put(settings).put("node.attr.zone", "a").build(),
            Settings.builder().put(settings).put("node.attr.zone", "b").build(),
            Settings.builder().put(settings).put("node.attr.zone", "c").build()
        );

        // Create index with 2 replicas and 2 search replicas
        assertThrows(IllegalArgumentException.class, () -> {
            createIndex(
                "test-idx",
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 2)
                    .build()
            );
        });
    }

    public void testAwarenessBalanceWithForcedAwarenessUpdateIndex() {
        Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values", "a,b,c")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), "true")
            .build();

        logger.info("--> starting 3 nodes on zones a,b,c");
        internalCluster().startNodes(
            Settings.builder().put(settings).put("node.attr.zone", "a").build(),
            Settings.builder().put(settings).put("node.attr.zone", "b").build(),
            Settings.builder().put(settings).put("node.attr.zone", "c").build()
        );

        // Create index with 2 replicas and 3 search replicas
        createIndex(
            "test-idx",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
                .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 3)
                .build()
        );

        // Update the number of search replicas to 4
        assertThrows(IllegalArgumentException.class, () -> {
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings("test-idx")
                    .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 4).build())
            );
        });
    }
}

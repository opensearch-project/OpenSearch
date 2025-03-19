/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
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
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
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

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.READER_WRITER_SPLIT_EXPERIMENTAL, Boolean.TRUE).build();
    }

    public void testAllocationAwarenessZones() {
        Settings commonSettings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values", "a,b")
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .build();

        logger.info("--> starting 8 nodes on different zones");
        List<String> nodes = internalCluster().startNodes(
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").build(),

            Settings.builder().put(commonSettings).put("node.attr.zone", "a").put(searchOnlyNode()).build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").put(searchOnlyNode()).build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "b").put(searchOnlyNode()).build(),
            Settings.builder().put(commonSettings).put("node.attr.zone", "a").put(searchOnlyNode()).build()
        );

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("8").execute().actionGet();
        assertThat(health.isTimedOut(), equalTo(false));

        logger.info("--> create index");
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5)
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

        assertThat(counts.get(nodes.get(3)), anyOf(equalTo(2), equalTo(3)));
        assertThat(counts.get(nodes.get(2)), anyOf(equalTo(2), equalTo(3)));
        assertThat(counts.get(nodes.get(0)), anyOf(equalTo(2), equalTo(3)));
        assertThat(counts.get(nodes.get(1)), anyOf(equalTo(2), equalTo(3)));
        assertThat(counts.get(nodes.get(4)), anyOf(equalTo(2), equalTo(3)));
        assertThat(counts.get(nodes.get(5)), anyOf(equalTo(2), equalTo(3)));
        assertThat(counts.get(nodes.get(6)), anyOf(equalTo(2), equalTo(3)));
        assertThat(counts.get(nodes.get(7)), anyOf(equalTo(2), equalTo(3)));
    }

    public void testAwarenessZonesIncrementalNodes() {
        Settings commonSettings = Settings.builder()
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b")
            .put("cluster.routing.allocation.awareness.attributes", "zone")
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
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5)
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
        assertThat(counts.get(nodes.get(0)), equalTo(5));
        assertThat(counts.get(nodes.get(1)), equalTo(5));
        assertThat(counts.get(nodes.get(2)), equalTo(5));
        assertThat(counts.get(nodes.get(3)), equalTo(5));

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

        assertThat(counts.get(nodes.get(0)), equalTo(5));
        assertThat(counts.get(nodes.get(1)), equalTo(3));
        assertThat(counts.get(nodes.get(2)), equalTo(5));
        assertThat(counts.get(nodes.get(3)), equalTo(3));
        assertThat(counts.get(B_2), equalTo(2));
        assertThat(counts.get(B_3), equalTo(2));

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

        assertThat(counts.get(nodes.get(0)), equalTo(5));
        assertThat(counts.get(nodes.get(1)), equalTo(3));
        assertThat(counts.get(nodes.get(2)), equalTo(5));
        assertThat(counts.get(nodes.get(3)), equalTo(3));
        assertThat(counts.get(B_2), equalTo(2));
        assertThat(counts.get(B_3), equalTo(2));
        assertThat(counts.containsKey(noZoneNode), equalTo(false));

        logger.info("--> Remove the awareness attribute setting");

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("cluster.routing.allocation.awareness.attributes", "").build())
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

        assertThat(counts.get(nodes.get(0)), equalTo(3));
        assertThat(counts.get(nodes.get(1)), equalTo(3));
        assertThat(counts.get(nodes.get(2)), equalTo(4));
        assertThat(counts.get(nodes.get(3)), equalTo(3));
        assertThat(counts.get(B_2), equalTo(2));
        assertThat(counts.get(B_3), equalTo(3));
        assertThat(counts.get(noZoneNode), equalTo(2));
    }

    public void testAwarenessBalanceWithForcedAwarenessCreateIndex() {
        Settings settings = Settings.builder()
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.balance", "true")
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
            .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
            .put("cluster.routing.allocation.awareness.attributes", "zone")
            .put("cluster.routing.allocation.awareness.balance", "true")
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

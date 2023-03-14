/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.action.OriginalIndicesTests;
import org.opensearch.action.search.SearchShardIterator;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.index.shard.ShardId;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.NodeNotConnectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonMap;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;

public class FailAwareWeightedRoutingTests extends OpenSearchTestCase {

    private ClusterState setUpCluster() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).build();

        // set up nodes
        DiscoveryNode nodeA = new DiscoveryNode(
            "node_zone_a",
            buildNewFakeTransportAddress(),
            singletonMap("zone", "a"),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        DiscoveryNode nodeB = new DiscoveryNode(
            "node_zone_b",
            buildNewFakeTransportAddress(),
            singletonMap("zone", "b"),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        DiscoveryNode nodeC = new DiscoveryNode(
            "node_zone_c",
            buildNewFakeTransportAddress(),
            singletonMap("zone", "c"),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );

        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.nodes());

        nodeBuilder.add(nodeA);
        nodeBuilder.add(nodeB);
        nodeBuilder.add(nodeC);
        clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();

        // set up weighted routing weights
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);
        WeightedRoutingMetadata weightedRoutingMetadata = new WeightedRoutingMetadata(weightedRouting, 0);
        Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
        metadataBuilder.putCustom(WeightedRoutingMetadata.TYPE, weightedRoutingMetadata);
        clusterState = ClusterState.builder(clusterState).metadata(metadataBuilder).build();

        return clusterState;

    }

    public void testFindNextWithoutFailOpen() throws IOException {

        ClusterState clusterState = setUpCluster();
        AtomicInteger shardSkipped = new AtomicInteger();
        // set up index
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 2)
                    .put(SETTING_CREATION_DATE, System.currentTimeMillis())
            )
            .build();

        ShardRouting shardRoutingA = TestShardRouting.newShardRouting("test", 0, "node_zone_a", true, ShardRoutingState.STARTED);
        ShardRouting shardRoutingB = TestShardRouting.newShardRouting("test", 0, "node_zone_b", false, ShardRoutingState.STARTED);
        ShardRouting shardRoutingC = TestShardRouting.newShardRouting("test", 0, "node_zone_c", false, ShardRoutingState.STARTED);

        Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
        metadataBuilder.put(indexMetadata, false).generateClusterUuidIfNeeded();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());

        final ShardId shardId = new ShardId("test", "_na_", 0);
        IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
        indexShardRoutingBuilder.addShard(shardRoutingA);
        indexShardRoutingBuilder.addShard(shardRoutingB);
        indexShardRoutingBuilder.addShard(shardRoutingC);

        indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilder.build());
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.add(indexRoutingTableBuilder.build());
        clusterState = ClusterState.builder(clusterState).routingTable(routingTableBuilder.build()).build();

        List<ShardRouting> shardRoutings = new ArrayList<>();
        shardRoutings.add(shardRoutingA);
        shardRoutings.add(shardRoutingB);
        shardRoutings.add(shardRoutingC);

        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        SearchShardIterator searchShardIterator = new SearchShardIterator(
            clusterAlias,
            shardId,
            shardRoutings,
            OriginalIndicesTests.randomOriginalIndices()
        );

        searchShardIterator.nextOrNull();
        searchShardIterator.nextOrNull();

        // fail open is not executed since fail open conditions don't met
        SearchShardTarget next = FailAwareWeightedRouting.getInstance()
            .findNext(searchShardIterator, clusterState, new OpenSearchRejectedExecutionException(), () -> shardSkipped.incrementAndGet());
        assertNull(next);
        assertEquals(1, shardSkipped.get());
    }

    public void testFindNextWithFailOpenDueTo5xx() throws IOException {

        ClusterState clusterState = setUpCluster();

        // set up index
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 2)
                    .put(SETTING_CREATION_DATE, System.currentTimeMillis())
            )
            .build();

        ShardRouting shardRoutingA = TestShardRouting.newShardRouting("test", 0, "node_zone_a", true, ShardRoutingState.STARTED);
        ShardRouting shardRoutingB = TestShardRouting.newShardRouting("test", 0, "node_zone_b", false, ShardRoutingState.STARTED);
        ShardRouting shardRoutingC = TestShardRouting.newShardRouting("test", 0, "node_zone_c", false, ShardRoutingState.STARTED);

        List<ShardRouting> shardRoutings = new ArrayList<>();
        shardRoutings.add(shardRoutingA);
        shardRoutings.add(shardRoutingB);
        shardRoutings.add(shardRoutingC);

        Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
        metadataBuilder.put(indexMetadata, false).generateClusterUuidIfNeeded();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());

        final ShardId shardId = new ShardId("test", "_na_", 0);
        IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);

        indexShardRoutingBuilder.addShard(shardRoutingA);
        indexShardRoutingBuilder.addShard(shardRoutingB);
        indexShardRoutingBuilder.addShard(shardRoutingC);

        indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilder.build());
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        routingTableBuilder.add(indexRoutingTableBuilder.build());
        clusterState = ClusterState.builder(clusterState).routingTable(routingTableBuilder.build()).build();

        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);

        SearchShardIterator searchShardIterator = new SearchShardIterator(
            clusterAlias,
            shardId,
            shardRoutings,
            OriginalIndicesTests.randomOriginalIndices()
        );

        searchShardIterator.nextOrNull();
        searchShardIterator.nextOrNull();

        // Node in zone b is disconnected
        DiscoveryNode node = clusterState.nodes().get("node_zone_b");
        // fail open is executed and shard present in node with weighted routing weight zero is returned
        SearchShardTarget next = FailAwareWeightedRouting.getInstance()
            .findNext(searchShardIterator, clusterState, new NodeNotConnectedException(node, "Node is not " + "connected"), () -> {});
        assertNotNull(next);
        assertEquals("node_zone_c", next.getNodeId());
    }

    public void testFindNextWithFailOpenDueToUnassignedShard() throws IOException {

        ClusterState clusterState = setUpCluster();
        AtomicInteger shardsSkipped = new AtomicInteger();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 2)
                    .put(SETTING_CREATION_DATE, System.currentTimeMillis())
            )
            .build();
        ShardRouting shardRoutingB = TestShardRouting.newShardRouting("test", 0, "node_zone_b", true, ShardRoutingState.STARTED);

        ShardRouting shardRoutingA = TestShardRouting.newShardRouting("test", 0, null, false, ShardRoutingState.UNASSIGNED);

        ShardRouting shardRoutingC = TestShardRouting.newShardRouting("test", 0, "node_zone_c", false, ShardRoutingState.STARTED);

        List<ShardRouting> shardRoutings = new ArrayList<>();
        shardRoutings.add(shardRoutingA);
        shardRoutings.add(shardRoutingB);
        shardRoutings.add(shardRoutingC);

        Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
        metadataBuilder.put(indexMetadata, false).generateClusterUuidIfNeeded();
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());

        final ShardId shardId = new ShardId("test", "_na_", 0);
        IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);

        indexShardRoutingBuilder.addShard(shardRoutingA);
        indexShardRoutingBuilder.addShard(shardRoutingB);
        indexShardRoutingBuilder.addShard(shardRoutingC);

        indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilder.build());
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        routingTableBuilder.add(indexRoutingTableBuilder.build());
        clusterState = ClusterState.builder(clusterState).routingTable(routingTableBuilder.build()).build();

        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);

        SearchShardIterator searchShardIterator = new SearchShardIterator(
            clusterAlias,
            shardId,
            shardRoutings,
            OriginalIndicesTests.randomOriginalIndices()
        );

        searchShardIterator.nextOrNull();
        searchShardIterator.nextOrNull();

        // since there is an unassigned shard in the cluster, fail open is executed and shard present in node with
        // weighted routing weight zero is returned
        SearchShardTarget next = FailAwareWeightedRouting.getInstance()
            .findNext(searchShardIterator, clusterState, new OpenSearchRejectedExecutionException(), () -> shardsSkipped.incrementAndGet());
        assertNotNull(next);
        assertEquals("node_zone_c", next.getNodeId());
        assertEquals(1, shardsSkipped.incrementAndGet());
    }
}

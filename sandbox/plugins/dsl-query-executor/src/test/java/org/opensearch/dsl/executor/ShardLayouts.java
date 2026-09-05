/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Cluster states with an <b>explicitly chosen</b> shard placement, for the tests that assert on the
 * shard-layout input of the fan-out width.
 */
final class ShardLayouts {

    private ShardLayouts() {}

    /** A routing service over stock settings — the same construction the coordinator's own read uses. */
    static OperationRouting routing() {
        return new OperationRouting(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    /**
     * A cluster state holding one index whose shard {@code i} sits on {@code placements.get(i)}.
     *
     * @param index the index name
     * @param placements node id per shard, in shard order; a {@code null} entry leaves that shard
     *                   unassigned
     * @param extraNodes further nodes that exist but hold no shard of this index
     * @return the state
     */
    static ClusterState clusterState(String index, List<String> placements, String... extraNodes) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        placements.stream().filter(node -> node != null).distinct().forEach(node -> nodes.add(newNode(node)));
        for (String extra : extraNodes) {
            if (placements.contains(extra) == false) {
                nodes.add(newNode(extra));
            }
        }
        nodes.localNodeId("node-0");
        nodes.clusterManagerNodeId("node-0");

        IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, placements.size())
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_CREATION_DATE, System.currentTimeMillis())
            )
            .build();
        Index resolved = indexMetadata.getIndex();

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(resolved);
        for (int shard = 0; shard < placements.size(); shard++) {
            ShardId shardId = new ShardId(resolved, shard);
            String node = placements.get(shard);
            IndexShardRoutingTable.Builder shardRouting = new IndexShardRoutingTable.Builder(shardId);
            shardRouting.addShard(
                node == null
                    ? TestShardRouting.newShardRouting(shardId, null, true, ShardRoutingState.UNASSIGNED)
                    : TestShardRouting.newShardRouting(shardId, node, true, ShardRoutingState.STARTED)
            );
            indexRoutingTable.addIndexShard(shardRouting.build());
        }

        return ClusterState.builder(new ClusterName("test"))
            .nodes(nodes)
            .metadata(Metadata.builder().put(indexMetadata, false).generateClusterUuidIfNeeded())
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();
    }

    private static DiscoveryNode newNode(String id) {
        return new DiscoveryNode(id, OpenSearchTestCase.buildNewFakeTransportAddress(), Version.CURRENT);
    }
}

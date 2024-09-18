/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.rest.RequestLimitSettings.CAT_INDICES_LIMIT_SETTING;
import static org.opensearch.rest.RequestLimitSettings.CAT_SEGMENTS_LIMIT_SETTING;
import static org.opensearch.rest.RequestLimitSettings.CAT_SHARDS_LIMIT_SETTING;

public class RequestLimitSettingsTests extends OpenSearchTestCase {

    public void testIsCircuitLimitBreached_forNullClusterState_expectNotBreached() {
        final Settings settings = Settings.builder().build();
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RequestLimitSettings requestLimitSettings = new RequestLimitSettings(clusterSettings, settings);
        final boolean breached = requestLimitSettings.isCircuitLimitBreached(null, RequestLimitSettings.BlockAction.CAT_INDICES);
        assertFalse(breached);
    }

    public void testIsCircuitLimitBreached_forCatIndicesWithSettingDisabled_expectNotBreached() {
        // Don't enable limit
        final Settings settings = Settings.builder().put(CAT_INDICES_LIMIT_SETTING.getKey(), -1).build();
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RequestLimitSettings requestLimitSettings = new RequestLimitSettings(clusterSettings, settings);
        final ClusterState clusterState = buildClusterState("test-index-1", "test-index-2", "test-index-3");
        final boolean breached = requestLimitSettings.isCircuitLimitBreached(clusterState, RequestLimitSettings.BlockAction.CAT_INDICES);
        assertFalse(breached);
    }

    public void testIsCircuitLimitBreached_forCatIndicesWithSettingEnabled_expectBreached() {
        // Set limit of 1 index
        final Settings settings = Settings.builder().put(CAT_INDICES_LIMIT_SETTING.getKey(), 1).build();
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RequestLimitSettings requestLimitSettings = new RequestLimitSettings(clusterSettings, settings);
        // Pass cluster state with 3 indices
        final ClusterState clusterState = buildClusterState("test-index-1", "test-index-2", "test-index-3");
        final boolean breached = requestLimitSettings.isCircuitLimitBreached(clusterState, RequestLimitSettings.BlockAction.CAT_INDICES);
        assertTrue(breached);
    }

    public void testIsCircuitLimitBreached_forCatIndicesWithSettingEnabled_expectNotBreached() {
        // Set limit of 5 indices
        final Settings settings = Settings.builder().put(CAT_INDICES_LIMIT_SETTING.getKey(), 5).build();
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RequestLimitSettings requestLimitSettings = new RequestLimitSettings(clusterSettings, settings);
        // Pass cluster state with 3 indices
        final ClusterState clusterState = buildClusterState("test-index-1", "test-index-2", "test-index-3");
        final boolean breached = requestLimitSettings.isCircuitLimitBreached(clusterState, RequestLimitSettings.BlockAction.CAT_INDICES);
        assertFalse(breached);
    }

    public void testIsCircuitLimitBreached_forCatShardsWithSettingDisabled_expectNotBreached() {
        // Don't enable limit
        final Settings settings = Settings.builder().put(CAT_SHARDS_LIMIT_SETTING.getKey(), -1).build();
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RequestLimitSettings requestLimitSettings = new RequestLimitSettings(clusterSettings, settings);
        // Build cluster state with 3 shards
        final ClusterState clusterState = buildClusterState("test-index-1", "test-index-2", "test-index-3");
        final boolean breached = requestLimitSettings.isCircuitLimitBreached(clusterState, RequestLimitSettings.BlockAction.CAT_SHARDS);
        assertFalse(breached);
    }

    public void testIsCircuitLimitBreached_forCatShardsWithSettingEnabled_expectBreached() {
        // Set limit of 2 shards
        final Settings settings = Settings.builder().put(CAT_SHARDS_LIMIT_SETTING.getKey(), 2).build();
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RequestLimitSettings requestLimitSettings = new RequestLimitSettings(clusterSettings, settings);
        // Build cluster state with 3 shards
        final ClusterState clusterState = buildClusterState("test-index-1", "test-index-2", "test-index-3");
        final boolean breached = requestLimitSettings.isCircuitLimitBreached(clusterState, RequestLimitSettings.BlockAction.CAT_SHARDS);
        assertTrue(breached);
    }

    public void testIsCircuitLimitBreached_forCatShardsWithSettingEnabled_expectNotBreached() {
        // Set limit of 3 shards
        final Settings settings = Settings.builder().put(CAT_SHARDS_LIMIT_SETTING.getKey(), 3).build();
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RequestLimitSettings requestLimitSettings = new RequestLimitSettings(clusterSettings, settings);
        // Build cluster state with 3 shards
        final ClusterState clusterState = buildClusterState("test-index-1", "test-index-2", "test-index-3");
        final boolean breached = requestLimitSettings.isCircuitLimitBreached(clusterState, RequestLimitSettings.BlockAction.CAT_SHARDS);
        assertFalse(breached);
    }

    public void testIsCircuitLimitBreached_forCatSegmentsWithSettingDisabled_expectNotBreached() {
        // Don't enable limit
        final Settings settings = Settings.builder().put(CAT_SEGMENTS_LIMIT_SETTING.getKey(), -1).build();
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RequestLimitSettings requestLimitSettings = new RequestLimitSettings(clusterSettings, settings);
        // Build cluster state with 3 indices
        final ClusterState clusterState = buildClusterState("test-index-1", "test-index-2", "test-index-3");
        final boolean breached = requestLimitSettings.isCircuitLimitBreached(clusterState, RequestLimitSettings.BlockAction.CAT_SEGMENTS);
        assertFalse(breached);
    }

    public void testIsCircuitLimitBreached_forCatSegmentsWithSettingEnabled_expectBreached() {
        // Set limit of 1 index
        final Settings settings = Settings.builder().put(CAT_SEGMENTS_LIMIT_SETTING.getKey(), 1).build();
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RequestLimitSettings requestLimitSettings = new RequestLimitSettings(clusterSettings, settings);
        // Build cluster state with 3 indices
        final ClusterState clusterState = buildClusterState("test-index-1", "test-index-2", "test-index-3");
        final boolean breached = requestLimitSettings.isCircuitLimitBreached(clusterState, RequestLimitSettings.BlockAction.CAT_SEGMENTS);
        assertTrue(breached);
    }

    public void testIsCircuitLimitBreached_forCatSegmentsWithSettingEnabled_expectNotBreached() {
        // Set limit of 3 indices
        final Settings settings = Settings.builder().put(CAT_SEGMENTS_LIMIT_SETTING.getKey(), 5).build();
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RequestLimitSettings requestLimitSettings = new RequestLimitSettings(clusterSettings, settings);
        // Build cluster state with 3 indices
        final ClusterState clusterState = buildClusterState("test-index-1", "test-index-2", "test-index-3");
        final boolean breached = requestLimitSettings.isCircuitLimitBreached(clusterState, RequestLimitSettings.BlockAction.CAT_SEGMENTS);
        assertFalse(breached);
    }

    private static ClusterState buildClusterState(String... indices) {
        final Metadata.Builder metadata = Metadata.builder();
        for (String index : indices) {
            metadata.put(IndexMetadata.builder(index).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0));
        }
        final Map<String, IndexRoutingTable> indexRoutingTableMap = new HashMap<>();
        for (String s : indices) {
            final Index index = new Index(s, "uuid");
            final ShardId shardId = new ShardId(index, 0);
            final ShardRouting primaryShardRouting = createShardRouting(shardId, true);
            final IndexShardRoutingTable.Builder indexShardRoutingTableBuilder = new IndexShardRoutingTable.Builder(shardId);
            indexShardRoutingTableBuilder.addShard(primaryShardRouting);
            final IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(primaryShardRouting)
                .addIndexShard(indexShardRoutingTableBuilder.build());
            indexRoutingTableMap.put(index.getName(), indexRoutingTable.build());
        }
        final RoutingTable routingTable = new RoutingTable(1, indexRoutingTableMap);
        return ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
    }

    private static ShardRouting createShardRouting(ShardId shardId, boolean isPrimary) {
        return TestShardRouting.newShardRouting(shardId, randomAlphaOfLength(4), isPrimary, ShardRoutingState.STARTED);
    }
}

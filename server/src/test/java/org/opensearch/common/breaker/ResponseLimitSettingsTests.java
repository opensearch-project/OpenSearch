/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.breaker;

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
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.common.breaker.ResponseLimitSettings.CAT_INDICES_RESPONSE_LIMIT_SETTING;
import static org.opensearch.common.breaker.ResponseLimitSettings.CAT_SEGMENTS_RESPONSE_LIMIT_SETTING;
import static org.opensearch.common.breaker.ResponseLimitSettings.CAT_SHARDS_RESPONSE_LIMIT_SETTING;

public class ResponseLimitSettingsTests extends OpenSearchTestCase {

    public void testIsResponseLimitBreachedForNullMetadataExpectNotBreached() {
        final Settings settings = Settings.builder().build();
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final ResponseLimitSettings responseLimitSettings = new ResponseLimitSettings(clusterSettings, settings);
        final boolean breached = ResponseLimitSettings.isResponseLimitBreached(
            (Metadata) null,
            ResponseLimitSettings.LimitEntity.INDICES,
            0
        );
        assertFalse(breached);
    }

    public void testIsResponseLimitBreachedForNullRoutingTableExpectNotBreached() {
        final Settings settings = Settings.builder().build();
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final ResponseLimitSettings responseLimitSettings = new ResponseLimitSettings(clusterSettings, settings);
        final boolean breached = ResponseLimitSettings.isResponseLimitBreached(
            (RoutingTable) null,
            ResponseLimitSettings.LimitEntity.INDICES,
            0
        );
        assertFalse(breached);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsResponseLimitBreachedForNullLimitEntityWithRoutingTableExpectException() {
        final ClusterState clusterState = buildClusterState("test-index-1", "test-index-2", "test-index-3");
        ResponseLimitSettings.isResponseLimitBreached(clusterState.getRoutingTable(), null, 4);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsResponseLimitBreachedForNullLimitEntityWithMetadataExpectException() {
        final ClusterState clusterState = buildClusterState("test-index-1", "test-index-2", "test-index-3");
        ResponseLimitSettings.isResponseLimitBreached(clusterState.getMetadata(), null, 4);
    }

    public void testIsResponseLimitBreachedForCatIndicesWithSettingDisabledExpectNotBreached() {
        // Don't enable limit
        final Settings settings = Settings.builder().put(CAT_INDICES_RESPONSE_LIMIT_SETTING.getKey(), -1).build();
        final boolean breached = isBreachedForIndices(settings);
        assertFalse(breached);
    }

    public void testIsResponseLimitBreachedForCatIndicesWithSettingEnabledExpectBreached() {
        // Set limit of 1 index
        final Settings settings = Settings.builder().put(CAT_INDICES_RESPONSE_LIMIT_SETTING.getKey(), 1).build();
        final boolean breached = isBreachedForIndices(settings);
        assertTrue(breached);
    }

    public void testIsResponseLimitBreachedForCatIndicesWithSettingEnabledExpectNotBreached() {
        // Set limit of 5 indices
        final Settings settings = Settings.builder().put(CAT_INDICES_RESPONSE_LIMIT_SETTING.getKey(), 5).build();
        final boolean breached = isBreachedForIndices(settings);
        assertFalse(breached);
    }

    private static boolean isBreachedForIndices(final Settings settings) {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final ResponseLimitSettings responseLimitSettings = new ResponseLimitSettings(clusterSettings, settings);
        // Pass cluster state with 3 indices
        final ClusterState clusterState = buildClusterState("test-index-1", "test-index-2", "test-index-3");
        return ResponseLimitSettings.isResponseLimitBreached(
            clusterState.getMetadata(),
            ResponseLimitSettings.LimitEntity.INDICES,
            responseLimitSettings.getCatIndicesResponseLimit()
        );
    }

    public void testIsResponseLimitBreachedForCatShardsWithSettingDisabledExpectNotBreached() {
        // Don't enable limit
        final Settings settings = Settings.builder().put(CAT_SHARDS_RESPONSE_LIMIT_SETTING.getKey(), -1).build();
        final boolean breached = isBreachedForShards(settings);
        assertFalse(breached);
    }

    private static boolean isBreachedForShards(Settings settings) {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final ResponseLimitSettings responseLimitSettings = new ResponseLimitSettings(clusterSettings, settings);
        // Build cluster state with 3 shards
        final ClusterState clusterState = buildClusterState("test-index-1", "test-index-2", "test-index-3");
        return ResponseLimitSettings.isResponseLimitBreached(
            clusterState.getRoutingTable(),
            ResponseLimitSettings.LimitEntity.SHARDS,
            responseLimitSettings.getCatShardsResponseLimit()
        );
    }

    public void testIsResponseLimitBreachedForCatShardsWithSettingEnabledExpectBreached() {
        // Set limit of 2 shards
        final Settings settings = Settings.builder().put(CAT_SHARDS_RESPONSE_LIMIT_SETTING.getKey(), 2).build();
        final boolean breached = isBreachedForShards(settings);
        assertTrue(breached);
    }

    public void testIsResponseLimitBreachedForCatShardsWithSettingEnabledExpectNotBreached() {
        // Set limit of 9 shards
        final Settings settings = Settings.builder().put(CAT_SHARDS_RESPONSE_LIMIT_SETTING.getKey(), 9).build();
        final boolean breached = isBreachedForShards(settings);
        assertFalse(breached);
    }

    public void testIsResponseLimitBreachedForCatSegmentsWithSettingDisabledExpectNotBreached() {
        // Don't enable limit
        final Settings settings = Settings.builder().put(CAT_SEGMENTS_RESPONSE_LIMIT_SETTING.getKey(), -1).build();
        final boolean breached = isBreachedForSegments(settings);
        assertFalse(breached);
    }

    private static boolean isBreachedForSegments(Settings settings) {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final ResponseLimitSettings responseLimitSettings = new ResponseLimitSettings(clusterSettings, settings);
        // Build cluster state with 3 indices
        final ClusterState clusterState = buildClusterState("test-index-1", "test-index-2", "test-index-3");
        final boolean breached = ResponseLimitSettings.isResponseLimitBreached(
            clusterState.getRoutingTable(),
            ResponseLimitSettings.LimitEntity.INDICES,
            responseLimitSettings.getCatSegmentsResponseLimit()
        );
        return breached;
    }

    public void testIsResponseLimitBreachedForCatSegmentsWithSettingEnabledExpectBreached() {
        // Set limit of 1 index
        final Settings settings = Settings.builder().put(CAT_SEGMENTS_RESPONSE_LIMIT_SETTING.getKey(), 1).build();
        final boolean breached = isBreachedForSegments(settings);
        assertTrue(breached);
    }

    public void testIsResponseLimitBreachedForCatSegmentsWithSettingEnabledExpectNotBreached() {
        // Set limit of 3 indices
        final Settings settings = Settings.builder().put(CAT_SEGMENTS_RESPONSE_LIMIT_SETTING.getKey(), 5).build();
        final boolean breached = isBreachedForSegments(settings);
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
            final ShardId primaryShardId = new ShardId(index, 0);
            final ShardRouting primaryShardRouting = createShardRouting(primaryShardId, true);
            final ShardId replicaShardId = new ShardId(index, 1);
            final ShardRouting replicaShardRouting = createShardRouting(replicaShardId, false);
            final IndexShardRoutingTable.Builder indexShardRoutingTableBuilder = new IndexShardRoutingTable.Builder(primaryShardId);
            indexShardRoutingTableBuilder.addShard(primaryShardRouting);
            indexShardRoutingTableBuilder.addShard(replicaShardRouting);
            final IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(primaryShardRouting)
                .addShard(replicaShardRouting)
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

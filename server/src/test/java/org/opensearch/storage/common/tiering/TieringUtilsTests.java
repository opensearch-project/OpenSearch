/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.common.tiering;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.storage.action.tiering.IndexTieringRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TieringUtilsTests extends OpenSearchTestCase {

    public void testGetTierPairForTargetTier_Warm() {
        String[] tiers = TieringUtils.getTierPairForTargetTier(IndexModule.TieringState.WARM);
        assertEquals("HOT", tiers[0]);
        assertEquals("WARM", tiers[1]);
    }

    public void testGetTierPairForTargetTier_Hot() {
        String[] tiers = TieringUtils.getTierPairForTargetTier(IndexModule.TieringState.HOT);
        assertEquals("WARM", tiers[0]);
        assertEquals("HOT", tiers[1]);
    }

    public void testGetTierPairForTargetTier_Invalid() {
        expectThrows(IllegalArgumentException.class, () -> TieringUtils.getTierPairForTargetTier(IndexModule.TieringState.HOT_TO_WARM));
    }

    public void testGetTieringStartTime() {
        Index index = new Index("test-index", "uuid");
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);

        when(clusterState.getMetadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(index)).thenReturn(indexMetadata);
        when(indexMetadata.getCustomData("tiering")).thenReturn(Map.of("h2w_start_time", "12345"));

        long startTime = TieringUtils.getTieringStartTime(clusterState, index, "h2w_start_time");
        assertEquals(12345L, startTime);
    }

    public void testGetTieringStartTime_NullCustomData() {
        Index index = new Index("test-index", "uuid");
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);

        when(clusterState.getMetadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(index)).thenReturn(indexMetadata);
        when(indexMetadata.getCustomData("tiering")).thenReturn(null);

        expectThrows(IllegalStateException.class, () -> TieringUtils.getTieringStartTime(clusterState, index, "h2w_start_time"));
    }

    public void testIsTerminalTier_Hot() {
        assertTrue(TieringUtils.isTerminalTier("HOT"));
        assertTrue(TieringUtils.isTerminalTier("hot"));
    }

    public void testIsTerminalTier_Warm() {
        assertTrue(TieringUtils.isTerminalTier("WARM"));
        assertTrue(TieringUtils.isTerminalTier("warm"));
    }

    public void testIsTerminalTier_Invalid() {
        assertFalse(TieringUtils.isTerminalTier("HOT_TO_WARM"));
        assertFalse(TieringUtils.isTerminalTier("random"));
    }

    public void testGetTieringStatefromTargetTier_Hot() {
        assertEquals(IndexModule.TieringState.WARM_TO_HOT, TieringUtils.getTieringStatefromTargetTier("HOT"));
    }

    public void testGetTieringStatefromTargetTier_Warm() {
        assertEquals(IndexModule.TieringState.HOT_TO_WARM, TieringUtils.getTieringStatefromTargetTier("WARM"));
    }

    public void testGetTieringStatefromTargetTier_Invalid() {
        expectThrows(IllegalArgumentException.class, () -> TieringUtils.getTieringStatefromTargetTier("HOT_TO_WARM"));
    }

    public void testGetTieringStatefromIndexSettings_HotToWarm() {
        org.opensearch.common.settings.Settings settings = org.opensearch.common.settings.Settings.builder()
            .put(IndexModule.INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT_TO_WARM.toString())
            .build();
        assertEquals(IndexModule.TieringState.HOT_TO_WARM, TieringUtils.getTieringStatefromIndexSettings(settings));
    }

    public void testGetTieringStatefromIndexSettings_WarmToHot() {
        org.opensearch.common.settings.Settings settings = org.opensearch.common.settings.Settings.builder()
            .put(IndexModule.INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.WARM_TO_HOT.toString())
            .build();
        assertEquals(IndexModule.TieringState.WARM_TO_HOT, TieringUtils.getTieringStatefromIndexSettings(settings));
    }

    public void testGetTieringStatefromIndexSettings_Warm() {
        org.opensearch.common.settings.Settings settings = org.opensearch.common.settings.Settings.builder()
            .put(IndexModule.INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.WARM.toString())
            .build();
        assertEquals(IndexModule.TieringState.HOT_TO_WARM, TieringUtils.getTieringStatefromIndexSettings(settings));
    }

    public void testGetTieringStatefromIndexSettings_Default() {
        org.opensearch.common.settings.Settings settings = org.opensearch.common.settings.Settings.EMPTY;
        assertEquals(IndexModule.TieringState.WARM_TO_HOT, TieringUtils.getTieringStatefromIndexSettings(settings));
    }

    public void testIsMigrationAllowed_AllowlistedIndices() {
        assertTrue(TieringUtils.isMigrationAllowed(".ds-my-data-stream-2023"));
        assertTrue(TieringUtils.isMigrationAllowed("my-index"));
    }

    public void testIsMigrationAllowed_BlocklistedIndices() {
        assertFalse(TieringUtils.isMigrationAllowed(".system-index"));
        assertFalse(TieringUtils.isMigrationAllowed(".kibana"));
    }

    public void testIsMigrationAllowed_InvalidInputs() {
        expectThrows(IllegalArgumentException.class, () -> TieringUtils.isMigrationAllowed(null));
        expectThrows(IllegalArgumentException.class, () -> TieringUtils.isMigrationAllowed(""));
        expectThrows(IllegalArgumentException.class, () -> TieringUtils.isMigrationAllowed(" "));
    }

    public void testGetTieringSourceType_FromRequest() {
        IndexTieringRequest warmRequest = new IndexTieringRequest("WARM", "test-index");
        assertEquals(TieringUtils.H2W_TIERING_TYPE_KEY, TieringUtils.getTieringSourceType(warmRequest));

        IndexTieringRequest hotRequest = new IndexTieringRequest("HOT", "test-index");
        assertEquals(TieringUtils.W2H_TIERING_TYPE_KEY, TieringUtils.getTieringSourceType(hotRequest));
    }

    public void testGetTieringSourceType_NullRequest() {
        expectThrows(IllegalArgumentException.class, () -> TieringUtils.getTieringSourceType((IndexTieringRequest) null));
    }

    public void testGetTieringSourceType_FromTieringState() {
        assertEquals(TieringUtils.H2W_TIERING_TYPE_KEY, TieringUtils.getTieringSourceType(IndexModule.TieringState.WARM));
        assertEquals(TieringUtils.W2H_TIERING_TYPE_KEY, TieringUtils.getTieringSourceType(IndexModule.TieringState.HOT));
    }

    public void testIsShardStateValidForTier_UnassignedReplica() {
        ShardRouting shard = mock(ShardRouting.class);
        ClusterState state = mock(ClusterState.class);
        when(shard.unassigned()).thenReturn(true);
        when(shard.primary()).thenReturn(false);
        assertTrue(TieringUtils.isShardStateValidForTier(shard, state, IndexModule.TieringState.WARM));
    }

    public void testIsShardStateValidForTier_UnassignedPrimary() {
        ShardRouting shard = mock(ShardRouting.class);
        ClusterState state = mock(ClusterState.class);
        when(shard.unassigned()).thenReturn(true);
        when(shard.primary()).thenReturn(true);
        assertFalse(TieringUtils.isShardStateValidForTier(shard, state, IndexModule.TieringState.WARM));
    }

    public void testIsShardStateValidForTier_StartedOnWarmNode_TargetWarm() {
        ShardRouting shard = mock(ShardRouting.class);
        ClusterState state = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        DiscoveryNode node = mock(DiscoveryNode.class);

        when(shard.unassigned()).thenReturn(false);
        when(shard.started()).thenReturn(true);
        when(shard.currentNodeId()).thenReturn("node1");
        when(state.getNodes()).thenReturn(nodes);
        when(nodes.get("node1")).thenReturn(node);
        when(node.isWarmNode()).thenReturn(true);

        assertTrue(TieringUtils.isShardStateValidForTier(shard, state, IndexModule.TieringState.WARM));
    }

    public void testIsShardStateValidForTier_StartedOnHotNode_TargetHot() {
        ShardRouting shard = mock(ShardRouting.class);
        ClusterState state = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        DiscoveryNode node = mock(DiscoveryNode.class);

        when(shard.unassigned()).thenReturn(false);
        when(shard.started()).thenReturn(true);
        when(shard.currentNodeId()).thenReturn("node1");
        when(state.getNodes()).thenReturn(nodes);
        when(nodes.get("node1")).thenReturn(node);
        when(node.isWarmNode()).thenReturn(false);

        assertTrue(TieringUtils.isShardStateValidForTier(shard, state, IndexModule.TieringState.HOT));
    }

    public void testIsShardStateValidForTier_InvalidTargetTier() {
        ShardRouting shard = mock(ShardRouting.class);
        ClusterState state = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        DiscoveryNode node = mock(DiscoveryNode.class);

        when(shard.unassigned()).thenReturn(false);
        when(shard.started()).thenReturn(true);
        when(shard.currentNodeId()).thenReturn("node1");
        when(state.getNodes()).thenReturn(nodes);
        when(nodes.get("node1")).thenReturn(node);
        when(node.isWarmNode()).thenReturn(false);

        expectThrows(
            IllegalArgumentException.class,
            () -> TieringUtils.isShardStateValidForTier(shard, state, IndexModule.TieringState.HOT_TO_WARM)
        );
    }

    public void testIsCCRFollowerIndex() {
        Settings ccrSettings = Settings.builder().put("index.plugins.replication.follower.leader_index", "leader-index").build();
        assertTrue(TieringUtils.isCCRFollowerIndex(ccrSettings));

        Settings nonCcrSettings = Settings.builder().build();
        assertFalse(TieringUtils.isCCRFollowerIndex(nonCcrSettings));
    }

    public void testTierValue() {
        assertEquals("hot", TieringUtils.Tier.HOT.value());
        assertEquals("warm", TieringUtils.Tier.WARM.value());
    }

    public void testTierFromString() {
        assertEquals(TieringUtils.Tier.HOT, TieringUtils.Tier.fromString("HOT"));
        assertEquals(TieringUtils.Tier.WARM, TieringUtils.Tier.fromString("warm"));
        expectThrows(IllegalArgumentException.class, () -> TieringUtils.Tier.fromString("invalid"));
        expectThrows(IllegalArgumentException.class, () -> TieringUtils.Tier.fromString(null));
    }
}

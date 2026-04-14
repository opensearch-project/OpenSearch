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
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;
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
}

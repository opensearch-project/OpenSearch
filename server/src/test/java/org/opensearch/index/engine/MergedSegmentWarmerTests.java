/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MergedSegmentWarmerTests extends OpenSearchTestCase {

    IndexShard mockIndexShard;
    MergedSegmentWarmer mergedSegmentWarmer;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockIndexShard = mock(IndexShard.class);
        when(mockIndexShard.shardId()).thenReturn(new ShardId(new Index("test-index", "_na_"), 0));
        mergedSegmentWarmer = new MergedSegmentWarmer(null, null, null, mockIndexShard);
    }

    public void testShouldWarm_warmerEnabled() {
        RecoverySettings mockRecoverySettings = mock(RecoverySettings.class);
        when(mockRecoverySettings.isMergedSegmentReplicationWarmerEnabled()).thenReturn(true);
        when(mockIndexShard.getRecoverySettings()).thenReturn(mockRecoverySettings);

        assertTrue(
            "MergedSegmentWarmer#shouldWarm is expected to return true when merged segment warmer is enabled",
            mergedSegmentWarmer.shouldWarm()
        );
    }

    public void testShouldWarm_warmerDisabled() {
        RecoverySettings mockRecoverySettings = mock(RecoverySettings.class);
        when(mockRecoverySettings.isMergedSegmentReplicationWarmerEnabled()).thenReturn(false);
        when(mockIndexShard.getRecoverySettings()).thenReturn(mockRecoverySettings);

        assertFalse(
            "MergedSegmentWarmer#shouldWarm is expected to return false when merged segment warmer is disabled",
            mergedSegmentWarmer.shouldWarm()
        );
    }
}

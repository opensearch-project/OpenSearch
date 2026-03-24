/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

public class ShardRoutingStateSplitTests extends OpenSearchTestCase {

    public void testSplittingStateValue() {
        assertEquals((byte) 5, ShardRoutingState.SPLITTING.value());
    }

    public void testFromValueSplitting() {
        assertEquals(ShardRoutingState.SPLITTING, ShardRoutingState.fromValue((byte) 5));
    }

    public void testFromValueInvalid() {
        expectThrows(IllegalStateException.class, () -> ShardRoutingState.fromValue((byte) 6));
    }

    public void testAllStatesRoundTrip() {
        for (ShardRoutingState state : ShardRoutingState.values()) {
            assertEquals(state, ShardRoutingState.fromValue(state.value()));
        }
    }

    public void testSplittingMethodReturnsTrueForSplittingState() {
        ShardRouting shard = TestShardRouting.newShardRouting(
            new ShardId(new Index("idx", "uuid"), 0),
            "node1",
            true,
            ShardRoutingState.SPLITTING
        );
        assertTrue(shard.splitting());
        assertFalse(shard.started());
        assertFalse(shard.relocating());
    }

    public void testSplittingMethodReturnsFalseForOtherStates() {
        ShardRouting started = TestShardRouting.newShardRouting(
            new ShardId(new Index("idx", "uuid"), 0),
            "node1",
            true,
            ShardRoutingState.STARTED
        );
        assertFalse(started.splitting());
    }

    public void testIsSplitTargetReturnsFalseForNormalShard() {
        ShardRouting shard = TestShardRouting.newShardRouting(
            new ShardId(new Index("idx", "uuid"), 0),
            "node1",
            true,
            ShardRoutingState.STARTED
        );
        assertFalse(shard.isSplitTarget());
        assertNull(shard.getParentShardId());
    }

    public void testGetRecoveringChildShardsReturnsNullForNonSplittingShard() {
        ShardRouting shard = TestShardRouting.newShardRouting(
            new ShardId(new Index("idx", "uuid"), 0),
            "node1",
            true,
            ShardRoutingState.STARTED
        );
        assertNull(shard.getRecoveringChildShards());
    }
}

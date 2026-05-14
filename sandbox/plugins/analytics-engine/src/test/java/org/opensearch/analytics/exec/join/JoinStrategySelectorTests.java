/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.calcite.rel.core.JoinRelType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class JoinStrategySelectorTests extends OpenSearchTestCase {

    /** Generous row threshold so size-only tests aren't constrained by it. */
    private static final long LARGE_ROW_THRESHOLD = Long.MAX_VALUE;

    public void testNonEquiJoinAlwaysCoordinatorCentric() {
        JoinStrategySelector selector = new JoinStrategySelector(
            100,
            LARGE_ROW_THRESHOLD,
            Map.of("a", new TableStatistics("a", 100, 10), "b", new TableStatistics("b", 100, 10))
        );
        assertEquals(
            JoinStrategy.COORDINATOR_CENTRIC,
            selector.selectStrategy("a", "b", /* isEqui */ false, JoinRelType.INNER)
        );
    }

    public void testInnerBroadcastWhenSmallerSideUnderThreshold() {
        // Both shards and rows must pass — small side: 1 shard, 100 rows.
        JoinStrategySelector selector = new JoinStrategySelector(
            2,
            LARGE_ROW_THRESHOLD,
            Map.of("small", new TableStatistics("small", 100, 1), "large", new TableStatistics("large", 1_000_000, 10))
        );
        assertEquals(JoinStrategy.BROADCAST, selector.selectStrategy("small", "large", true, JoinRelType.INNER));
        // Order of arguments must not matter — INNER's eligible build is whichever has fewer rows.
        assertEquals(JoinStrategy.BROADCAST, selector.selectStrategy("large", "small", true, JoinRelType.INNER));
    }

    public void testInnerHashShuffleWhenBothSidesLargeByShards() {
        JoinStrategySelector selector = new JoinStrategySelector(
            2,
            LARGE_ROW_THRESHOLD,
            Map.of("left", new TableStatistics("left", 100, 10), "right", new TableStatistics("right", 100, 10))
        );
        assertEquals(JoinStrategy.HASH_SHUFFLE, selector.selectStrategy("left", "right", true, JoinRelType.INNER));
    }

    public void testUnknownIndexTreatedAsLarge() {
        // Missing stats → shardCount Integer.MAX_VALUE + rowCount 0 → fail-safe HASH_SHUFFLE.
        JoinStrategySelector selector = new JoinStrategySelector(2, LARGE_ROW_THRESHOLD, Map.of());
        assertEquals(
            JoinStrategy.HASH_SHUFFLE,
            selector.selectStrategy("unknown-left", "unknown-right", true, JoinRelType.INNER)
        );
    }

    public void testInnerBuildSideByRowCountWhenAvailable() {
        JoinStrategySelector selector = new JoinStrategySelector(
            2,
            LARGE_ROW_THRESHOLD,
            Map.of("a", new TableStatistics("a", 1000, 5), "b", new TableStatistics("b", 200, 5))
        );
        assertEquals("right", selector.selectBuildSide("a", "b", JoinRelType.INNER));
        assertEquals("left", selector.selectBuildSide("b", "a", JoinRelType.INNER));
    }

    public void testInnerBuildSideFallsBackToShardCountWhenRowsMissing() {
        JoinStrategySelector selector = new JoinStrategySelector(
            2,
            LARGE_ROW_THRESHOLD,
            Map.of("a", new TableStatistics("a", 0, 10), "b", new TableStatistics("b", 0, 3))
        );
        assertEquals("right", selector.selectBuildSide("a", "b", JoinRelType.INNER));
    }

    // ---- Row gate (new) ----

    /**
     * Even when shards are tiny, an oversized row count must refuse broadcast — that's the
     * regression this gate was added for.
     */
    public void testInnerHashShuffleWhenSmallShardsButLargeRows() {
        JoinStrategySelector selector = new JoinStrategySelector(
            10,                    // generous shard threshold
            1_000_000L,            // 1M row cap
            Map.of("small", new TableStatistics("small", 50_000_000L, 1), "huge", new TableStatistics("huge", 100_000_000L, 5))
        );
        assertEquals(
            JoinStrategy.HASH_SHUFFLE,
            selector.selectStrategy("small", "huge", true, JoinRelType.INNER)
        );
    }

    /**
     * Fail-safe: when row count is unknown (zero), broadcast is refused regardless of shard count.
     */
    public void testRefusesBroadcastWhenRowCountUnknown() {
        JoinStrategySelector selector = new JoinStrategySelector(
            10,
            1_000_000L,
            Map.of("a", new TableStatistics("a", 0L, 1), "b", new TableStatistics("b", 0L, 1))
        );
        assertEquals(JoinStrategy.HASH_SHUFFLE, selector.selectStrategy("a", "b", true, JoinRelType.INNER));
    }

    /**
     * Both gates must hold simultaneously — passing one but failing the other refuses broadcast.
     */
    public void testBothGatesRequiredSimultaneously() {
        // Passes shard gate but fails row gate.
        JoinStrategySelector rowFails = new JoinStrategySelector(
            10,
            100L,
            Map.of("a", new TableStatistics("a", 1000L, 1), "b", new TableStatistics("b", 1000L, 1))
        );
        assertEquals(JoinStrategy.HASH_SHUFFLE, rowFails.selectStrategy("a", "b", true, JoinRelType.INNER));

        // Passes row gate but fails shard gate.
        JoinStrategySelector shardFails = new JoinStrategySelector(
            1,
            1_000_000L,
            Map.of("a", new TableStatistics("a", 100L, 5), "b", new TableStatistics("b", 100L, 5))
        );
        assertEquals(JoinStrategy.HASH_SHUFFLE, shardFails.selectStrategy("a", "b", true, JoinRelType.INNER));
    }

    /**
     * Regression for the smaller-side-only gate bug: for INNER, if the side that's smaller by
     * rows fails one gate but the other side fits BOTH, the selector must still pick BROADCAST
     * (and use the fitting side as the build). Refusing broadcast just because the preferred
     * side failed would silently degrade skewed-but-broadcast-viable INNER joins.
     */
    public void testInnerFallsBackToOtherSideWhenSmallerFailsButLargerFits() {
        // small-rows-side has 100 shards (fails shard gate); other side has 5 shards + slightly
        // more rows but still under the row threshold (fits both gates).
        JoinStrategySelector selector = new JoinStrategySelector(
            10,
            1_000_000L,
            Map.of(
                "smallRowsManyShards",
                new TableStatistics("smallRowsManyShards", 1_000L, 100),
                "moreRowsFewShards",
                new TableStatistics("moreRowsFewShards", 5_000L, 5)
            )
        );
        assertEquals(
            "INNER must broadcast on the side that fits both gates",
            JoinStrategy.BROADCAST,
            selector.selectStrategy("smallRowsManyShards", "moreRowsFewShards", true, JoinRelType.INNER)
        );
        // selectBuildSide must mirror selectStrategy and pick the fitting side as the build.
        assertEquals(
            "build side must be the side that fits both gates, not the smaller side",
            "right",
            selector.selectBuildSide("smallRowsManyShards", "moreRowsFewShards", JoinRelType.INNER)
        );
        // Argument order must not affect the decision.
        assertEquals(
            JoinStrategy.BROADCAST,
            selector.selectStrategy("moreRowsFewShards", "smallRowsManyShards", true, JoinRelType.INNER)
        );
        assertEquals(
            "build side flips when arg order flips",
            "left",
            selector.selectBuildSide("moreRowsFewShards", "smallRowsManyShards", JoinRelType.INNER)
        );
    }

    /**
     * INNER with both sides fitting both gates: prefer the smaller side as the build. Validates
     * that the "either side" fast path still optimizes for the cheapest broadcast payload.
     */
    public void testInnerPrefersSmallerSideWhenBothFit() {
        JoinStrategySelector selector = new JoinStrategySelector(
            10,
            1_000_000L,
            Map.of("small", new TableStatistics("small", 100L, 1), "smaller", new TableStatistics("smaller", 50L, 1))
        );
        assertEquals(JoinStrategy.BROADCAST, selector.selectStrategy("small", "smaller", true, JoinRelType.INNER));
        // 'smaller' has fewer rows (50 vs 100) → it is the build side regardless of arg order.
        assertEquals("smaller side wins when both fit", "right", selector.selectBuildSide("small", "smaller", JoinRelType.INNER));
        assertEquals("smaller side wins regardless of arg order", "left", selector.selectBuildSide("smaller", "small", JoinRelType.INNER));
    }

    // ---- Outer-join correctness ----

    /**
     * LEFT OUTER preserves the left side, so the build MUST be the right side regardless of
     * which side is smaller. Broadcast is viable only if right's shard count + row count fit.
     */
    public void testLeftOuterForcesRightAsBuild() {
        JoinStrategySelector selector = new JoinStrategySelector(
            2,
            LARGE_ROW_THRESHOLD,
            // Under INNER we'd build left (smaller rows), but LEFT OUTER must build right.
            Map.of("left", new TableStatistics("left", 100L, 1), "right", new TableStatistics("right", 1000L, 2))
        );
        assertEquals(JoinStrategy.BROADCAST, selector.selectStrategy("left", "right", true, JoinRelType.LEFT));
        assertEquals("right", selector.selectBuildSide("left", "right", JoinRelType.LEFT));
    }

    /**
     * LEFT OUTER with right too big → fall back. INNER would still pick BROADCAST because left
     * is small, but LEFT OUTER cannot use left as the build side.
     */
    public void testLeftOuterFallsBackWhenRightTooBig() {
        JoinStrategySelector selector = new JoinStrategySelector(
            2,
            LARGE_ROW_THRESHOLD,
            Map.of("left", new TableStatistics("left", 100L, 1), "right", new TableStatistics("right", 1000L, 10))
        );
        assertEquals(JoinStrategy.HASH_SHUFFLE, selector.selectStrategy("left", "right", true, JoinRelType.LEFT));
        // Sanity check: same stats with INNER would broadcast (left as smaller eligible build).
        assertEquals(JoinStrategy.BROADCAST, selector.selectStrategy("left", "right", true, JoinRelType.INNER));
    }

    /**
     * RIGHT OUTER is the mirror — preserves the right side, build must be left.
     */
    public void testRightOuterForcesLeftAsBuild() {
        JoinStrategySelector selector = new JoinStrategySelector(
            2,
            LARGE_ROW_THRESHOLD,
            Map.of("left", new TableStatistics("left", 1000L, 2), "right", new TableStatistics("right", 100L, 1))
        );
        assertEquals(JoinStrategy.BROADCAST, selector.selectStrategy("left", "right", true, JoinRelType.RIGHT));
        assertEquals("left", selector.selectBuildSide("left", "right", JoinRelType.RIGHT));
    }

    /**
     * FULL OUTER preserves both sides → no eligible build side → always fall back, regardless
     * of size.
     */
    public void testFullOuterAlwaysFallsBack() {
        JoinStrategySelector selector = new JoinStrategySelector(
            100,
            LARGE_ROW_THRESHOLD,
            Map.of("a", new TableStatistics("a", 100L, 1), "b", new TableStatistics("b", 100L, 1))
        );
        assertEquals(JoinStrategy.HASH_SHUFFLE, selector.selectStrategy("a", "b", true, JoinRelType.FULL));
    }

    /**
     * SEMI / ANTI joins emit only left-side rows, semantically left-preserving for purposes
     * of broadcast — build must be right.
     */
    public void testSemiAndAntiForceRightAsBuild() {
        JoinStrategySelector selector = new JoinStrategySelector(
            2,
            LARGE_ROW_THRESHOLD,
            Map.of("left", new TableStatistics("left", 100L, 1), "right", new TableStatistics("right", 100L, 1))
        );
        assertEquals("right", selector.selectBuildSide("left", "right", JoinRelType.SEMI));
        assertEquals("right", selector.selectBuildSide("left", "right", JoinRelType.ANTI));
    }
}

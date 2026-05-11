/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class JoinStrategySelectorTests extends OpenSearchTestCase {

    public void testNonEquiJoinAlwaysCoordinatorCentric() {
        JoinStrategySelector selector = new JoinStrategySelector(
            100,
            Map.of("a", new TableStatistics("a", 0, 10), "b", new TableStatistics("b", 0, 10))
        );
        assertEquals(JoinStrategy.COORDINATOR_CENTRIC, selector.selectStrategy("a", "b", /* isEqui */ false));
    }

    public void testBroadcastWhenSmallerSideUnderThreshold() {
        JoinStrategySelector selector = new JoinStrategySelector(
            2,
            Map.of("small", new TableStatistics("small", 0, 1), "large", new TableStatistics("large", 0, 10))
        );
        assertEquals(JoinStrategy.BROADCAST, selector.selectStrategy("small", "large", true));
        // Order of arguments must not matter — selection uses min() of shard counts.
        assertEquals(JoinStrategy.BROADCAST, selector.selectStrategy("large", "small", true));
    }

    public void testHashShuffleWhenBothSidesLarge() {
        JoinStrategySelector selector = new JoinStrategySelector(
            2,
            Map.of("left", new TableStatistics("left", 0, 10), "right", new TableStatistics("right", 0, 10))
        );
        assertEquals(JoinStrategy.HASH_SHUFFLE, selector.selectStrategy("left", "right", true));
    }

    public void testUnknownIndexTreatedAsLarge() {
        // Missing stats → shardCount falls back to Integer.MAX_VALUE → HASH_SHUFFLE.
        JoinStrategySelector selector = new JoinStrategySelector(2, Map.of());
        assertEquals(JoinStrategy.HASH_SHUFFLE, selector.selectStrategy("unknown-left", "unknown-right", true));
    }

    public void testBuildSideByRowCountWhenAvailable() {
        JoinStrategySelector selector = new JoinStrategySelector(
            2,
            Map.of("a", new TableStatistics("a", 1000, 5), "b", new TableStatistics("b", 200, 5))
        );
        // b has fewer rows, build b.
        assertEquals("right", selector.selectBuildSide("a", "b"));
        assertEquals("left", selector.selectBuildSide("b", "a"));
    }

    public void testBuildSideFallsBackToShardCountWhenRowsMissing() {
        JoinStrategySelector selector = new JoinStrategySelector(
            2,
            Map.of("a", new TableStatistics("a", 0, 10), "b", new TableStatistics("b", 0, 3))
        );
        assertEquals("right", selector.selectBuildSide("a", "b"));
    }
}

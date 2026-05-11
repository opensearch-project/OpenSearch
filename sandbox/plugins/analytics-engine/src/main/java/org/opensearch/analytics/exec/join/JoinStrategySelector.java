/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * Picks a {@link JoinStrategy} for a binary join based on per-index statistics and a broadcast
 * threshold. Ported from OLAP's {@code CostEstimator} shape.
 *
 * <ul>
 *   <li>If the smaller side's shard count is ≤ {@code broadcastMaxShards}, choose BROADCAST.</li>
 *   <li>Else choose HASH_SHUFFLE (M2) — until M2 lands, callers treat HASH_SHUFFLE as a signal to
 *       fall back to COORDINATOR_CENTRIC.</li>
 *   <li>Non-equi (theta) joins always return COORDINATOR_CENTRIC regardless of stats.</li>
 * </ul>
 *
 * <p>Cost-based build-side selection ({@link #selectBuildSide}) uses row count when both sides have
 * CBO stats, falling back to shard count otherwise.
 *
 * @opensearch.internal
 */
public final class JoinStrategySelector {

    private static final Logger LOGGER = LogManager.getLogger(JoinStrategySelector.class);

    private final int broadcastMaxShards;
    private final Map<String, TableStatistics> statsByIndex;

    public JoinStrategySelector(int broadcastMaxShards, Map<String, TableStatistics> statsByIndex) {
        this.broadcastMaxShards = broadcastMaxShards;
        this.statsByIndex = statsByIndex == null ? Map.of() : statsByIndex;
    }

    /** Pick a strategy for an equi-join on {@code leftIndex}/{@code rightIndex}. */
    public JoinStrategy selectStrategy(String leftIndex, String rightIndex, boolean isEqui) {
        if (!isEqui) {
            return JoinStrategy.COORDINATOR_CENTRIC;
        }
        int leftShards = shardCount(leftIndex);
        int rightShards = shardCount(rightIndex);
        int smallerShards = Math.min(leftShards, rightShards);

        LOGGER.info(
            "Join strategy: left={} ({} shards), right={} ({} shards), broadcastThreshold={}",
            leftIndex,
            leftShards,
            rightIndex,
            rightShards,
            broadcastMaxShards
        );

        if (smallerShards <= broadcastMaxShards) {
            return JoinStrategy.BROADCAST;
        }
        return JoinStrategy.HASH_SHUFFLE;
    }

    /**
     * Decide which side to broadcast (the smaller one). Returns {@code "left"} or {@code "right"}.
     * Uses row counts when both sides have CBO stats; falls back to shard counts otherwise.
     */
    public String selectBuildSide(String leftIndex, String rightIndex) {
        long leftRows = rowCount(leftIndex);
        long rightRows = rowCount(rightIndex);
        if (leftRows > 0 && rightRows > 0) {
            return leftRows <= rightRows ? "left" : "right";
        }
        int leftShards = shardCount(leftIndex);
        int rightShards = shardCount(rightIndex);
        return leftShards <= rightShards ? "left" : "right";
    }

    public int broadcastMaxShards() {
        return broadcastMaxShards;
    }

    private int shardCount(String indexName) {
        TableStatistics stats = statsByIndex.get(indexName);
        if (stats != null && stats.shardCount() > 0) {
            return stats.shardCount();
        }
        LOGGER.warn("No shard count for index [{}], assuming large (Integer.MAX_VALUE) — will pick HASH_SHUFFLE", indexName);
        return Integer.MAX_VALUE;
    }

    private long rowCount(String indexName) {
        TableStatistics stats = statsByIndex.get(indexName);
        return stats != null ? stats.rowCount() : 0;
    }
}

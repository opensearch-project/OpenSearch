/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * Picks a {@link JoinStrategy} for a binary join based on per-index statistics and a broadcast
 * threshold. Ported from OLAP's {@code CostEstimator} shape.
 *
 * <p>Strategy is gated by join type and BOTH size dimensions (shards + rows):
 * <ul>
 *   <li>Non-equi (theta) joins always return {@link JoinStrategy#COORDINATOR_CENTRIC}.</li>
 *   <li>For broadcast-to-probe correctness, the build side must NOT be a row-preservation
 *       side. Per join type, the eligible build is fixed:
 *       <ul>
 *         <li>{@code INNER} — either side; size wins.</li>
 *         <li>{@code LEFT OUTER} / {@code SEMI} / {@code ANTI} — right only.</li>
 *         <li>{@code RIGHT OUTER} — left only.</li>
 *         <li>{@code FULL OUTER} — neither side; falls back unconditionally.</li>
 *       </ul>
 *   </li>
 *   <li>Broadcast requires BOTH the eligible side's shard count {@code ≤ broadcastMaxShards}
 *       AND its row count {@code ≤ broadcastMaxRows}. When the row count is unknown
 *       (stats missing or zero), broadcast is refused fail-safe — coordinator-centric is
 *       always correct, broadcast for an oversized build is not.</li>
 * </ul>
 *
 * <p>{@link #selectBuildSide} respects the eligible-side rule — it never returns the
 * row-preservation side for an outer join.
 *
 * @opensearch.internal
 */
public final class JoinStrategySelector {

    private static final Logger LOGGER = LogManager.getLogger(JoinStrategySelector.class);

    private final int broadcastMaxShards;
    private final long broadcastMaxRows;
    private final Map<String, TableStatistics> statsByIndex;

    public JoinStrategySelector(int broadcastMaxShards, long broadcastMaxRows, Map<String, TableStatistics> statsByIndex) {
        this.broadcastMaxShards = broadcastMaxShards;
        this.broadcastMaxRows = broadcastMaxRows;
        this.statsByIndex = statsByIndex == null ? Map.of() : statsByIndex;
    }

    /**
     * Pick a strategy for an equi-join on {@code leftIndex}/{@code rightIndex}. The {@code joinType}
     * argument constrains which side may be the build (see class javadoc).
     */
    public JoinStrategy selectStrategy(String leftIndex, String rightIndex, boolean isEqui, JoinRelType joinType) {
        if (!isEqui) {
            return JoinStrategy.COORDINATOR_CENTRIC;
        }
        if (joinType == JoinRelType.FULL) {
            // FULL OUTER preserves both sides — no eligible build for broadcast-to-probe.
            LOGGER.info("Join strategy: FULL OUTER not eligible for broadcast; falling through to HASH_SHUFFLE");
            return JoinStrategy.HASH_SHUFFLE;
        }
        int leftShards = shardCount(leftIndex);
        int rightShards = shardCount(rightIndex);
        long leftRows = rowCount(leftIndex);
        long rightRows = rowCount(rightIndex);

        String eligibleBuild = eligibleBuildSide(joinType);

        LOGGER.info(
            "Join strategy: type={} left={} ({} shards, {} rows), right={} ({} shards, {} rows), "
                + "eligibleBuild={}, shardThreshold={}, rowThreshold={}",
            joinType,
            leftIndex,
            leftShards,
            leftRows,
            rightIndex,
            rightShards,
            rightRows,
            eligibleBuild,
            broadcastMaxShards,
            broadcastMaxRows
        );

        // Both gates must pass. Row count <= 0 means "unknown" — refuse broadcast fail-safe
        // because we cannot bound the build-side payload.
        //
        // For INNER (eligibleBuild == "either") the smaller side is preferred — broadcasting
        // the smaller payload is always cheaper. But if the smaller side fails one gate while
        // the larger side fits BOTH gates, broadcast is still viable using the larger side as
        // the build. Refusing broadcast just because the preferred side failed would be a
        // strategy-selection regression for skewed inputs (e.g. fewer rows but many shards on
        // one side, slightly more rows but a single shard on the other).
        if ("either".equals(eligibleBuild)) {
            boolean leftFits = fitsBroadcastGates(leftShards, leftRows);
            boolean rightFits = fitsBroadcastGates(rightShards, rightRows);
            if (leftFits || rightFits) {
                return JoinStrategy.BROADCAST;
            }
            if (leftRows <= 0 && rightRows <= 0) {
                LOGGER.info("Join strategy: row count unknown for both inner-join sides; refusing broadcast fail-safe");
            }
            return JoinStrategy.HASH_SHUFFLE;
        }

        int eligibleBuildShards;
        long eligibleBuildRows;
        if ("left".equals(eligibleBuild)) {
            eligibleBuildShards = leftShards;
            eligibleBuildRows = leftRows;
        } else { // "right"
            eligibleBuildShards = rightShards;
            eligibleBuildRows = rightRows;
        }
        if (fitsBroadcastGates(eligibleBuildShards, eligibleBuildRows)) {
            return JoinStrategy.BROADCAST;
        }
        if (eligibleBuildRows <= 0) {
            LOGGER.info("Join strategy: row count unknown for the eligible build side; refusing broadcast fail-safe");
        }
        return JoinStrategy.HASH_SHUFFLE;
    }

    private boolean fitsBroadcastGates(int shards, long rows) {
        return shards <= broadcastMaxShards && rows > 0 && rows <= broadcastMaxRows;
    }

    /**
     * Decide which side to broadcast. For outer / semi / anti joins the row-preservation side
     * is forced as probe so the build is fixed by join type. For INNER, prefer the smaller
     * side when both fit the broadcast gates; if only one side fits, that side is the build —
     * matching the "either side" eligibility logic in {@link #selectStrategy}.
     *
     * @return {@code "left"} or {@code "right"}.
     */
    public String selectBuildSide(String leftIndex, String rightIndex, JoinRelType joinType) {
        String eligible = eligibleBuildSide(joinType);
        if (!"either".equals(eligible)) {
            return eligible;
        }
        int leftShards = shardCount(leftIndex);
        int rightShards = shardCount(rightIndex);
        long leftRows = rowCount(leftIndex);
        long rightRows = rowCount(rightIndex);
        boolean leftFits = fitsBroadcastGates(leftShards, leftRows);
        boolean rightFits = fitsBroadcastGates(rightShards, rightRows);
        if (leftFits && !rightFits) {
            return "left";
        }
        if (rightFits && !leftFits) {
            return "right";
        }
        // Both fit (or neither — caller for the latter case is selectStrategy and it would have
        // already returned HASH_SHUFFLE; selectBuildSide is also called from advisor pre-tag
        // contexts so we keep a sensible fallback). Prefer smaller side: rows when known, else
        // shards.
        if (leftRows > 0 && rightRows > 0) {
            return leftRows <= rightRows ? "left" : "right";
        }
        return leftShards <= rightShards ? "left" : "right";
    }

    /**
     * Returns the build-side constraint for {@code joinType}: {@code "either"}, {@code "left"},
     * or {@code "right"}. Callers must ensure FULL OUTER never reaches this — that case has no
     * eligible build under broadcast-to-probe semantics and should be filtered out earlier.
     */
    private static String eligibleBuildSide(JoinRelType joinType) {
        if (joinType == null || joinType == JoinRelType.INNER) {
            return "either";
        }
        switch (joinType) {
            case LEFT:
            case SEMI:
            case ANTI:
                // Probe = left (preserved); build must be right.
                return "right";
            case RIGHT:
                // Probe = right (preserved); build must be left.
                return "left";
            case FULL:
                throw new IllegalStateException(
                    "FULL OUTER has no eligible broadcast build side; selectStrategy should fall back before reaching here"
                );
            default:
                // Unknown variant — be conservative.
                return "either";
        }
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

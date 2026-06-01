/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

/**
 * Strategy for distributed join + aggregate execution. Despite living in the {@code exec.join}
 * package (historical), this enum carries values for both join strategies (M0/M1/M2) and the
 * M3 hash-shuffle aggregate path. The {@link MppStrategyMetrics} counters and
 * {@code GET /_analytics/_strategies} endpoint surface dispatch counts for all of them.
 *
 * <p>The coordinator selects one of these after the CBO plan is produced; strategy selection
 * is not a planning decision (see design note in {@code 65-mpp-joins-design.md §4}).
 *
 * @opensearch.internal
 */
public enum MppStrategy {

    /** Both sides reduced to coordinator (SINGLETON), join runs there. Safe default. Also
     *  used as the agg-side fallback when the cost model picks coord-centric over the M3
     *  hash-shuffle alternative. */
    COORDINATOR_CENTRIC,

    /** Small (build) side broadcast to all probe-side data nodes; join runs in parallel on each. */
    BROADCAST,

    /** Both sides hash-partitioned by join key and shuffled to workers (M2). */
    HASH_SHUFFLE,

    /** Hash-shuffle aggregate (M3): partial-aggregate output hash-partitioned by group keys
     *  and shuffled to a worker tier where FINAL runs in parallel. Sibling of HASH_SHUFFLE
     *  but for aggregates — the dispatch path is {@code HashShuffleAggregateDispatch}, not
     *  {@code HashShuffleDispatch}. */
    HASH_SHUFFLE_AGG
}

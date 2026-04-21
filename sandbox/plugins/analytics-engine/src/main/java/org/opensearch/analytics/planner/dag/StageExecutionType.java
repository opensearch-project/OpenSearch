/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

/**
 * Classifies a stage by its execution shape (dispatch target + response shape),
 * used by {@code StageExecutionBuilder} to look up the scheduler for the stage.
 *
 * @opensearch.internal
 */
public enum StageExecutionType {
    /**
     * Fragment dispatched per-shard to data nodes; responses are row batches.
     * Used by scans, filters, partial aggregates, and anything else that runs
     * a shard-local fragment producing rows.
     */
    SHARD_FRAGMENT,
    /**
     * Runs at the coordinator with a backend-provided {@code ExchangeSink}
     * (from {@code ExchangeSinkProvider}). Used for final aggregation, sort,
     * and any residual coordinator-side reduction the backend owns. Inputs
     * are child stage outputs (either rows from SHARD_SCAN or shuffle reads).
     */
    COORDINATOR_REDUCE,
    /**
     * Runs at the coordinator as a pure gather — no backend compute, no
     * fragment dispatch. The walker attaches a {@code RowProducingSink} and
     * accumulates child output directly. Used only for root/parent gather
     * stages sitting above children that already produced the final rows.
     * A single-stage query that scans shards is {@link #SHARD_FRAGMENT}, not this.
     */
    LOCAL_PASSTHROUGH,
    /**
     * Fragment dispatched per-shard to data nodes; responses are hash-partitioned
     * shuffle manifests (not row batches). Used by the producer side of a
     * HASH_DISTRIBUTED exchange. Scheduler NYI — registry throws until landed.
     */
    SHUFFLE_WRITE,
    /**
     * Fragment dispatched per-shard to data nodes; responses are broadcast
     * handles (not row batches). Used by the producer side of a
     * BROADCAST_DISTRIBUTED exchange. Scheduler NYI — registry throws until landed.
     */
    BROADCAST_WRITE
}

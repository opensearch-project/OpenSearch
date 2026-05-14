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
     * and any residual coordinator-side reduction the backend owns. Child
     * SHARD_FRAGMENT stage outputs are fed into the sink.
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
     * Late materialization (QTF) root stage. Consumes the reduced output from
     * a child COORDINATOR_REDUCE stage, builds a position map from (shard_id, __row_id__),
     * dispatches fetch-by-row-id requests per shard, and assembles the final result.
     */
    LATE_MATERIALIZATION
}

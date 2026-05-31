/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Can-match pre-filter phase for the analytics engine.
 *
 * <p>Before dispatching fragment execution to data-node shards, the coordinator
 * sends a lightweight can-match request to each target shard. The data node
 * checks Parquet row-group metadata statistics (min/max per column) against
 * the query's filter predicates. Shards where no row group can possibly match
 * are eliminated from fragment dispatch, avoiding expensive full-scan execution.
 *
 * <h2>Architecture</h2>
 * <pre>
 * Coordinator (before fragment dispatch):
 *   1. Resolve target shards (TargetResolver)
 *   2. For each shard: send AnalyticsCanMatchRequest
 *   3. Data node: check Parquet footer stats against predicates
 *   4. Response: canMatch=true/false + optional min/max for sort optimization
 *   5. Filter out canMatch=false shards
 *   6. Build StageTask list only for matching shards
 * </pre>
 *
 * <h2>Use Cases</h2>
 * <ul>
 *   <li>Time-range queries on time-partitioned indices ({@code WHERE timestamp > '2026-01-01'})</li>
 *   <li>Wildcard index patterns ({@code index-2026*}) where most shards are out of range</li>
 *   <li>Term filters on low-cardinality columns with known min/max per shard</li>
 * </ul>
 */
package org.opensearch.analytics.exec.canmatch;

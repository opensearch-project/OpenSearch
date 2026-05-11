/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

/**
 * Per-index statistics used by {@link JoinStrategySelector} to pick a join strategy and
 * by {@code CostEstimator}-style logic to pick a build side.
 *
 * <p>Row count may be 0 when statistics are unavailable — callers should fall back to
 * {@code shardCount} in that case. Shard count is always non-zero (derived from
 * {@code ClusterState} metadata).
 *
 * @opensearch.internal
 */
public record TableStatistics(String indexName, long rowCount, int shardCount) {}

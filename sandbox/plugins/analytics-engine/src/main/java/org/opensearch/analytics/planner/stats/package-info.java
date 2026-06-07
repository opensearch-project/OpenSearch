/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Index statistics consumed by the analytics-engine cost model. {@code StatisticsCollector} reads
 * per-index shard counts (and, in future, row counts) into {@code TableStatistics} for plan-shape
 * cost decisions such as biasing toward the aggregate-split shape.
 */
package org.opensearch.analytics.planner.stats;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Cluster-wide stats rollup for the analytics engine.
 * {@link org.opensearch.analytics.stats.AnalyticsStatsCollector} folds each
 * completed {@link org.opensearch.analytics.exec.profile.QueryProfile} into
 * cumulative counters and HdrHistogram-backed latency buckets.
 * {@link org.opensearch.analytics.stats.RestAnalyticsStatsAction} exposes a
 * fan-out snapshot at {@code GET _plugins/_analytics/stats} via
 * {@link org.opensearch.analytics.stats.transport.AnalyticsStatsAction}.
 */
package org.opensearch.analytics.stats;

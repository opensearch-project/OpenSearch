/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Node-local stats rollup for the analytics engine.
 * {@link org.opensearch.analytics.stats.AnalyticsStatsCollector} accumulates
 * lifecycle events from the {@link org.opensearch.analytics.backend.AnalyticsOperationListener}
 * firing sites; {@link org.opensearch.analytics.stats.RestAnalyticsStatsAction}
 * exposes a snapshot at {@code GET _plugins/_analytics/stats}.
 */
package org.opensearch.analytics.stats;

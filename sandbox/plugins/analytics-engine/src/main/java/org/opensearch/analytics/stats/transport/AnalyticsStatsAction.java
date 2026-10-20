/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.stats.transport;

import org.opensearch.action.ActionType;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Transport action for the cluster-wide analytics-engine stats rollup. Backs
 * {@code GET _plugins/_analytics/stats} via {@link TransportAnalyticsStatsAction}.
 *
 * <p>Cluster-monitor scope so the action goes through the standard nodes-stats
 * authorization path.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class AnalyticsStatsAction extends ActionType<AnalyticsStatsResponse> {

    public static final AnalyticsStatsAction INSTANCE = new AnalyticsStatsAction();
    public static final String NAME = "cluster:monitor/analytics/stats";

    private AnalyticsStatsAction() {
        super(NAME, AnalyticsStatsResponse::new);
    }
}

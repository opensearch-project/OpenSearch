/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.settings;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;

import java.util.concurrent.TimeUnit;

import static org.opensearch.plugin.insights.core.service.TopQueriesByLatencyService.DEFAULT_TOP_N_SIZE;
import static org.opensearch.plugin.insights.core.service.TopQueriesByLatencyService.DEFAULT_WINDOW_SIZE;

/**
 * Settings for Query Insights Plugin
 *
 * @opensearch.api
 * @opensearch.experimental
 */
public class QueryInsightsSettings {
    /**
     * Query Insights base uri
     */
    public static final String PLUGINS_BASE_URI = "/_insights";

    /**
     * Settings for Top Queries
     *
     */
    public static final String TOP_QUERIES_BASE_URI = PLUGINS_BASE_URI + "/top_queries";
    public static final String TOP_N_QUERIES_SETTING_PREFIX = "search.top_n_queries";

    public static final String TOP_N_LATENCY_QUERIES_PREFIX = TOP_N_QUERIES_SETTING_PREFIX + ".latency";
    /**
     * Boolean setting for enabling top queries by latency.
     */
    public static final Setting<Boolean> TOP_N_LATENCY_QUERIES_ENABLED = Setting.boolSetting(
        TOP_N_LATENCY_QUERIES_PREFIX + ".enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Int setting to define the top n size for top queries by latency.
     */
    public static final Setting<Integer> TOP_N_LATENCY_QUERIES_SIZE = Setting.intSetting(
        TOP_N_LATENCY_QUERIES_PREFIX + ".top_n_size",
        DEFAULT_TOP_N_SIZE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Time setting to define the window size in seconds for top queries by latency.
     */
    public static final Setting<TimeValue> TOP_N_LATENCY_QUERIES_WINDOW_SIZE = Setting.positiveTimeSetting(
        TOP_N_LATENCY_QUERIES_PREFIX + ".window_size",
        new TimeValue(DEFAULT_WINDOW_SIZE, TimeUnit.SECONDS),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Default constructor
     */
    public QueryInsightsSettings() {}
}

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
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporterType;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Settings for Query Insights Plugin
 *
 * @opensearch.api
 * @opensearch.experimental
 */
public class QueryInsightsSettings {
    /**
     * Default Values and Settings
     */
    public static final TimeValue MAX_WINDOW_SIZE = new TimeValue(21600, TimeUnit.SECONDS);
    public static final int MAX_N_SIZE = 100;
    public static final TimeValue MIN_EXPORT_INTERVAL = new TimeValue(21600, TimeUnit.SECONDS);
    public static final String DEFAULT_LOCAL_INDEX_MAPPING = "mappings/top_n_queries_record.json";
    /** Default window size in seconds to keep the top N queries with latency data in query insight store */
    public static final int DEFAULT_WINDOW_SIZE = 60;
    /** Default top N size to keep the data in query insight store */
    public static final int DEFAULT_TOP_N_SIZE = 3;
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
     * Settings for exporters
     */
    public static final String TOP_N_LATENCY_QUERIES_EXPORTER_PREFIX = TOP_N_LATENCY_QUERIES_PREFIX + ".exporter";

    /**
     * Boolean setting for enabling top queries by latency exporter
     */
    public static final Setting<Boolean> TOP_N_LATENCY_QUERIES_EXPORTER_ENABLED = Setting.boolSetting(
        TOP_N_LATENCY_QUERIES_EXPORTER_PREFIX + ".enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Setting for top queries by latency exporter type
     */
    public static final Setting<QueryInsightsExporterType> TOP_N_LATENCY_QUERIES_EXPORTER_TYPE = new Setting<>(
        TOP_N_LATENCY_QUERIES_EXPORTER_PREFIX + ".type",
        QueryInsightsExporterType.LOCAL_INDEX.name(),
        QueryInsightsExporterType::parse,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Setting for top queries by latency exporter interval
     */
    public static final Setting<TimeValue> TOP_N_LATENCY_QUERIES_EXPORTER_INTERVAL = Setting.positiveTimeSetting(
        TOP_N_LATENCY_QUERIES_EXPORTER_PREFIX + ".interval",
        MIN_EXPORT_INTERVAL,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Setting for identifier (e.g. index name for index exporter) top queries by latency exporter
     * Default value is "top_queries_since_{current_timestamp}"
     */
    public static final Setting<String> TOP_N_LATENCY_QUERIES_EXPORTER_IDENTIFIER = Setting.simpleString(
        TOP_N_LATENCY_QUERIES_EXPORTER_PREFIX + ".identifier",
        "top_queries_since_" + System.currentTimeMillis(),
        value -> {
            if (value == null || value.length() == 0) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Invalid index name for [%s]", TOP_N_LATENCY_QUERIES_EXPORTER_PREFIX + ".identifier")
                );
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Default constructor
     */
    public QueryInsightsSettings() {}
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.settings;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugin.insights.core.exporter.SinkType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Settings for Query Insights Plugin
 *
 * @opensearch.api
 * @opensearch.experimental
 */
public class QueryInsightsSettings {
    /**
     * Executors settings
     */
    public static final String QUERY_INSIGHTS_EXECUTOR = "query_insights_executor";
    /**
     * Max number of thread
     */
    public static final int MAX_THREAD_COUNT = 5;
    /**
     * Max number of requests for the consumer to collect at one time
     */
    public static final int QUERY_RECORD_QUEUE_CAPACITY = 1000;
    /**
     * Time interval for record queue consumer to run
     */
    public static final TimeValue QUERY_RECORD_QUEUE_DRAIN_INTERVAL = new TimeValue(5, TimeUnit.SECONDS);
    /**
     * Default Values and Settings
     */
    public static final TimeValue MAX_WINDOW_SIZE = new TimeValue(1, TimeUnit.DAYS);
    /**
     * Minimal window size
     */
    public static final TimeValue MIN_WINDOW_SIZE = new TimeValue(1, TimeUnit.MINUTES);
    /**
     * Valid window sizes
     */
    public static final Set<TimeValue> VALID_WINDOW_SIZES_IN_MINUTES = new HashSet<>(
        Arrays.asList(
            new TimeValue(1, TimeUnit.MINUTES),
            new TimeValue(5, TimeUnit.MINUTES),
            new TimeValue(10, TimeUnit.MINUTES),
            new TimeValue(30, TimeUnit.MINUTES)
        )
    );

    /** Default N size for top N queries */
    public static final int MAX_N_SIZE = 100;
    /** Default window size in seconds to keep the top N queries with latency data in query insight store */
    public static final TimeValue DEFAULT_WINDOW_SIZE = new TimeValue(60, TimeUnit.SECONDS);
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
    /** Default prefix for top N queries feature */
    public static final String TOP_N_QUERIES_SETTING_PREFIX = "search.insights.top_queries";
    /** Default prefix for top N queries by latency feature */
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
        DEFAULT_WINDOW_SIZE,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Config key for exporter type
     */
    public static final String EXPORTER_TYPE = "type";
    /**
     * Config key for export index
     */
    public static final String EXPORT_INDEX = "config.index";

    /**
     * Settings and defaults for top queries exporters
     */
    private static final String TOP_N_LATENCY_QUERIES_EXPORTER_PREFIX = TOP_N_LATENCY_QUERIES_PREFIX + ".exporter.";
    /**
     * Default index pattern of top n queries by latency
     */
    public static final String DEFAULT_TOP_N_LATENCY_QUERIES_INDEX_PATTERN = "'top_queries_by_latency-'YYYY.MM.dd";
    /**
     * Default exporter type of top queries
     */
    public static final String DEFAULT_TOP_QUERIES_EXPORTER_TYPE = SinkType.LOCAL_INDEX.toString();

    /**
     * Settings for the exporter of top latency queries
     */
    public static final Setting<Settings> TOP_N_LATENCY_EXPORTER_SETTINGS = Setting.groupSetting(
        TOP_N_LATENCY_QUERIES_EXPORTER_PREFIX,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Default constructor
     */
    public QueryInsightsSettings() {}
}

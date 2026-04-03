/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.slowlogs;

import org.opensearch.common.logging.SlowLogLevel;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.shard.SearchOperationListener;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tiered storage search slow log. Logs slow queries and fetches with tiered storage per-query metrics.
 * Constructor will accept IndexSettings and register dynamic update consumers for all threshold settings.
 * SearchOperationListener hooks (onPreQueryPhase, onQueryPhase, onPreFetchPhase, onFetchPhase,
 * onPreSliceExecution, onSliceExecution, etc.) and slow log printing logic
 * will be added in the implementation PR.
 */
public final class TieredStorageSearchSlowLog implements SearchOperationListener {

    /** Prefix for all tiered storage search slow log settings. */
    public static final String TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX = "index.tiered.storage.slowlog";

    /** Setting to enable or disable tiered storage search slow logging. */
    public static final Setting<Boolean> TIERED_STORAGE_SEARCH_SLOWLOG_ENABLED = Setting.boolSetting(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".enabled",
        false,
        Setting.Property.Dynamic,
        Property.IndexScope
    );
    /** Threshold setting for query phase warn-level slow log. */
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING = Setting.timeSetting(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".threshold.query.warn",
        TimeValue.timeValueMillis(10000),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );
    /** Threshold setting for query phase info-level slow log. */
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING = Setting.timeSetting(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".threshold.query.info",
        TimeValue.timeValueMillis(5000),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );
    /** Threshold setting for query phase debug-level slow log. */
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING = Setting.timeSetting(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".threshold.query.debug",
        TimeValue.timeValueMillis(2000),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );
    /** Threshold setting for query phase trace-level slow log. */
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING = Setting.timeSetting(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".threshold.query.trace",
        TimeValue.timeValueMillis(500),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );
    /** Threshold setting for fetch phase warn-level slow log. */
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING = Setting.timeSetting(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.warn",
        TimeValue.timeValueMillis(10000),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );
    /** Threshold setting for fetch phase info-level slow log. */
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING = Setting.timeSetting(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.info",
        TimeValue.timeValueMillis(5000),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );
    /** Threshold setting for fetch phase debug-level slow log. */
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING = Setting.timeSetting(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.debug",
        TimeValue.timeValueMillis(2000),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );
    /** Threshold setting for fetch phase trace-level slow log. */
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING = Setting.timeSetting(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.trace",
        TimeValue.timeValueMillis(500),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );
    /** Setting for the slow log level. */
    public static final Setting<SlowLogLevel> INDEX_SEARCH_SLOWLOG_LEVEL = new Setting<>(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".level",
        SlowLogLevel.TRACE.name(),
        SlowLogLevel::parse,
        Property.Dynamic,
        Property.IndexScope
    );

    /** Map of setting keys to their corresponding Setting objects for tiered storage search slow log. */
    public static final Map<String, Setting<?>> TIERED_STORAGE_SEARCH_SLOWLOG_SETTINGS_MAP = Collections.unmodifiableMap(new HashMap<>() {
        {
            put(TIERED_STORAGE_SEARCH_SLOWLOG_ENABLED.getKey(), TIERED_STORAGE_SEARCH_SLOWLOG_ENABLED);
            put(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING.getKey(), INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING);
            put(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING.getKey(), INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING);
            put(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING.getKey(), INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING);
            put(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING.getKey(), INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING);
            put(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING.getKey(), INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING);
            put(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING.getKey(), INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING);
            put(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING.getKey(), INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING);
            put(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING.getKey(), INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING);
            put(INDEX_SEARCH_SLOWLOG_LEVEL.getKey(), INDEX_SEARCH_SLOWLOG_LEVEL);
        }
    });

    /** Set of all tiered storage search slow log settings. */
    public static final Set<Setting<?>> TIERED_STORAGE_SEARCH_SLOWLOG_SETTINGS = Collections.unmodifiableSet(
        new HashSet<>(TIERED_STORAGE_SEARCH_SLOWLOG_SETTINGS_MAP.values())
    );

    /** Constructs a new TieredStorageSearchSlowLog. */
    public TieredStorageSearchSlowLog() {}

    /**
     * Formats slow log messages with tiered storage per-query metrics as JSON.
     * Full toXContent serialization and toString formatting will be added in the implementation PR.
     */
    static final class TieredStorageSlowLogPrinter {}
}

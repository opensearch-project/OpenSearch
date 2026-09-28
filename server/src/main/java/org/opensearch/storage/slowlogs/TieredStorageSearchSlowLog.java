/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.slowlogs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.logging.SlowLogLevel;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Search slow log implementation for tiered storage (warm data).
 * Logs slow queries and fetches with per-query cache and prefetch metrics.
 *
 * @opensearch.experimental
 */
public final class TieredStorageSearchSlowLog implements SearchOperationListener {

    private volatile boolean tieredStorageSlowlogEnabled;
    private volatile long queryWarnThreshold;
    private volatile long queryInfoThreshold;
    private volatile long queryDebugThreshold;
    private volatile long queryTraceThreshold;

    private volatile long fetchWarnThreshold;
    private volatile long fetchInfoThreshold;
    private volatile long fetchDebugThreshold;
    private volatile long fetchTraceThreshold;

    private SlowLogLevel level;

    private final Logger queryLogger;
    private final Logger fetchLogger;

    /** Settings prefix for tiered storage search slow log. */
    public static final String TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX = "index.tiered.storage.slowlog";

    /** Setting to enable or disable tiered storage search slow log. */
    public static final Setting<Boolean> TIERED_STORAGE_SEARCH_SLOWLOG_ENABLED = Setting.boolSetting(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".enabled",
        false,
        Property.Dynamic,
        Property.IndexScope
    );

    /** Query warn threshold setting. */
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING = Setting.timeSetting(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".threshold.query.warn",
        TimeValue.timeValueMillis(10000),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );

    /** Query info threshold setting. */
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING = Setting.timeSetting(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".threshold.query.info",
        TimeValue.timeValueMillis(5000),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );

    /** Query debug threshold setting. */
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING = Setting.timeSetting(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".threshold.query.debug",
        TimeValue.timeValueMillis(2000),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );

    /** Query trace threshold setting. */
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING = Setting.timeSetting(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".threshold.query.trace",
        TimeValue.timeValueMillis(500),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );

    /** Fetch warn threshold setting. */
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING = Setting.timeSetting(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.warn",
        TimeValue.timeValueMillis(10000),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );

    /** Fetch info threshold setting. */
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING = Setting.timeSetting(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.info",
        TimeValue.timeValueMillis(5000),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );

    /** Fetch debug threshold setting. */
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING = Setting.timeSetting(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.debug",
        TimeValue.timeValueMillis(2000),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );

    /** Fetch trace threshold setting. */
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING = Setting.timeSetting(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.trace",
        TimeValue.timeValueMillis(500),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );

    /** Slow log level setting. */
    public static final Setting<SlowLogLevel> INDEX_SEARCH_SLOWLOG_LEVEL = new Setting<>(
        TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".level",
        SlowLogLevel.TRACE.name(),
        SlowLogLevel::parse,
        Property.Dynamic,
        Property.IndexScope
    );

    /** Map of all tiered storage search slow log settings keyed by setting name. */
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

    private static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));

    /**
     * Creates a new TieredStorageSearchSlowLog instance.
     * @param indexSettings the index settings
     */
    public TieredStorageSearchSlowLog(IndexSettings indexSettings) {
        this.queryLogger = LogManager.getLogger(TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".query");
        this.fetchLogger = LogManager.getLogger(TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".fetch");

        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(TIERED_STORAGE_SEARCH_SLOWLOG_ENABLED, this::setTieredStorageSlowlogEnabled);
        setTieredStorageSlowlogEnabled(indexSettings.getValue(TIERED_STORAGE_SEARCH_SLOWLOG_ENABLED));

        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING, this::setQueryWarnThreshold);
        setQueryWarnThreshold(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING));
        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING, this::setQueryInfoThreshold);
        setQueryInfoThreshold(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING));
        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING, this::setQueryDebugThreshold);
        setQueryDebugThreshold(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING));
        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING, this::setQueryTraceThreshold);
        setQueryTraceThreshold(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING));

        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING, this::setFetchWarnThreshold);
        setFetchWarnThreshold(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING));
        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING, this::setFetchInfoThreshold);
        setFetchInfoThreshold(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING));
        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING, this::setFetchDebugThreshold);
        setFetchDebugThreshold(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING));
        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING, this::setFetchTraceThreshold);
        setFetchTraceThreshold(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING));

        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_LEVEL, this::setLevel);
        setLevel(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_LEVEL));
    }

    private void setLevel(SlowLogLevel level) {
        this.level = level;
        Loggers.setLevel(queryLogger, level.name());
        Loggers.setLevel(fetchLogger, level.name());
    }

    private TieredStoragePerQueryMetric removeMetricCollector() {
        return TieredStorageQueryMetricService.getInstance().removeMetricCollector(Thread.currentThread().threadId());
    }

    private Set<TieredStoragePerQueryMetric> removeMetricCollectors(String parentTaskId, String shardId, boolean isQueryPhase) {
        return TieredStorageQueryMetricService.getInstance().removeMetricCollectors(parentTaskId, shardId, isQueryPhase);
    }

    private void setMetricCollector(SearchContext searchContext, boolean isQueryPhase) {
        final SearchShardTask searchTask = searchContext.getTask();
        final Logger log = isQueryPhase ? queryLogger : fetchLogger;
        if (searchTask == null) {
            log.error("Warm Slow Log: Search Task not expected to be null");
        }
        TieredStorageQueryMetricService.getInstance()
            .addMetricCollector(
                Thread.currentThread().threadId(),
                new TieredStoragePerQueryMetricImpl(
                    searchTask == null ? null : searchTask.getParentTaskId().toString(),
                    searchContext.shardTarget().getShardId().toString()
                ),
                isQueryPhase
            );
    }

    @Override
    public void onPreQueryPhase(SearchContext searchContext) {
        // The same search thread can pick up multiple slice executions post https://github.com/apache/lucene/pull/13472
        // so we initialize collectors only in onPreSliceExecution
    }

    @Override
    public void onFailedQueryPhase(SearchContext searchContext) {
        // Only clean up if we were collecting metrics
        if (tieredStorageSlowlogEnabled) {
            removeMetricCollector();
            removeMetricCollectors(
                searchContext.getTask().getParentTaskId().toString(),
                searchContext.shardTarget().getShardId().toString(),
                true
            );
        }
    }

    @Override
    public void onQueryPhase(SearchContext context, long tookInNanos) {
        // Get all collectors associated with the task/shard
        final List<TieredStoragePerQueryMetric> metricCollectors = new ArrayList<>(
            removeMetricCollectors(context.getTask().getParentTaskId().toString(), context.shardTarget().getShardId().toString(), true)
        );

        // No need to call removeMetricCollector() here as that will be handled in onSliceExecution in both
        // concurrent search and non-concurrent search cases

        // Only log if tiered storage slow log is enabled
        if (tieredStorageSlowlogEnabled) {
            printSlowLog(
                context,
                tookInNanos,
                metricCollectors,
                queryWarnThreshold,
                queryLogger,
                queryInfoThreshold,
                queryDebugThreshold,
                queryTraceThreshold
            );
        }
    }

    private void printSlowLog(
        SearchContext context,
        long tookInNanos,
        List<TieredStoragePerQueryMetric> metricCollectors,
        long warnThreshold,
        Logger log,
        long infoThreshold,
        long debugThreshold,
        long traceThreshold
    ) {
        if (warnThreshold >= 0 && tookInNanos > warnThreshold) {
            log.warn("{}", new TieredStorageSlowLogPrinter(context, tookInNanos, metricCollectors));
        } else if (infoThreshold >= 0 && tookInNanos > infoThreshold) {
            log.info("{}", new TieredStorageSlowLogPrinter(context, tookInNanos, metricCollectors));
        } else if (debugThreshold >= 0 && tookInNanos > debugThreshold) {
            log.debug("{}", new TieredStorageSlowLogPrinter(context, tookInNanos, metricCollectors));
        } else if (traceThreshold >= 0 && tookInNanos > traceThreshold) {
            log.trace("{}", new TieredStorageSlowLogPrinter(context, tookInNanos, metricCollectors));
        }
    }

    @Override
    public void onPreSliceExecution(SearchContext searchContext) {
        // Only collect metrics if tiered storage slow log is enabled
        if (tieredStorageSlowlogEnabled) {
            setMetricCollector(searchContext, true);
        }
    }

    @Override
    public void onFailedSliceExecution(SearchContext searchContext) {
        // Only clean up if we were collecting metrics
        if (tieredStorageSlowlogEnabled) {
            removeMetricCollector();
        }
    }

    @Override
    public void onSliceExecution(SearchContext searchContext) {
        // Only clean up if we were collecting metrics
        if (tieredStorageSlowlogEnabled) {
            removeMetricCollector();
        }
    }

    @Override
    public void onPreFetchPhase(SearchContext searchContext) {
        // Fetch phase execution is starting. Add new metric collector only if enabled
        if (tieredStorageSlowlogEnabled) {
            setMetricCollector(searchContext, false);
        }
    }

    @Override
    public void onFailedFetchPhase(SearchContext searchContext) {
        // Only clean up if we were collecting metrics
        if (tieredStorageSlowlogEnabled) {
            removeMetricCollector();
            removeMetricCollectors(
                searchContext.getTask().getParentTaskId().toString(),
                searchContext.shardTarget().getShardId().toString(),
                false
            );
        }
    }

    @Override
    public void onFetchPhase(SearchContext context, long tookInNanos) {
        // Only clean up and log if we were collecting metrics
        if (tieredStorageSlowlogEnabled) {
            removeMetricCollector();
            // Although fetch phase is single threaded today, we will use the same map implementation for posterity.
            // It's also much cleaner than propagating the fetch boolean to TieredStorageQueryMetricService
            final List<TieredStoragePerQueryMetric> metricCollectors = new ArrayList<>(
                removeMetricCollectors(context.getTask().getParentTaskId().toString(), context.shardTarget().getShardId().toString(), false)
            );
            assert metricCollectors.size() == 1 : "Fetch phase is expected to be single threaded, so we should only have 1 collector";

            printSlowLog(
                context,
                tookInNanos,
                metricCollectors,
                fetchWarnThreshold,
                fetchLogger,
                fetchInfoThreshold,
                fetchDebugThreshold,
                fetchTraceThreshold
            );
        }
    }

    /**
     * Formats slow log output as JSON with warm storage metrics.
     *
     * @opensearch.experimental
     */
    static final class TieredStorageSlowLogPrinter {
        private final SearchContext context;
        private final long tookInNanos;
        private final List<TieredStoragePerQueryMetric> metricCollectors;
        private final Logger logger = LogManager.getLogger(TieredStorageSlowLogPrinter.class);

        /**
         * Creates a new slow log printer.
         * @param context the search context
         * @param tookInNanos the time taken in nanoseconds
         * @param metricCollectors the per-query metric collectors
         */
        TieredStorageSlowLogPrinter(SearchContext context, long tookInNanos, List<TieredStoragePerQueryMetric> metricCollectors) {
            this.context = context;
            this.tookInNanos = tookInNanos;
            this.metricCollectors = metricCollectors;
        }

        @Override
        public String toString() {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                toXContent(builder, FORMAT_PARAMS);
                return builder.toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();

            // warm_stats array
            builder.startArray("warm_stats");
            if (metricCollectors != null && !metricCollectors.isEmpty()) {
                for (TieredStoragePerQueryMetric collector : metricCollectors) {
                    builder.value(collector.toString());
                }
            }
            builder.endArray();

            // took and took_millis
            builder.field("took", TimeValue.timeValueNanos(tookInNanos).toString());
            builder.field("took_millis", TimeUnit.NANOSECONDS.toMillis(tookInNanos));

            // stats array
            builder.startArray("stats");
            List<Object> stats = new ArrayList<>();
            if (context.groupStats() != null) {
                if (metricCollectors != null) {
                    stats.addAll(metricCollectors);
                }
                stats.addAll(Objects.requireNonNull(context.groupStats()));
            }
            if (!stats.isEmpty()) {
                for (Object stat : stats) {
                    builder.value(stat.toString());
                }
            }
            builder.endArray();

            // search_type, total_shards, and source
            builder.field("search_type", context.searchType().toString());
            builder.field("total_shards", context.numberOfShards());

            if (context.request().source() != null) {
                builder.field("source", context.request().source().toString(params));
            } else {
                builder.nullField("source");
            }

            builder.endObject();
            return builder;
        }
    }

    /**
     * Sets whether tiered storage slow log is enabled.
     * @param tieredStorageSlowlogEnabled true to enable
     */
    public void setTieredStorageSlowlogEnabled(boolean tieredStorageSlowlogEnabled) {
        this.tieredStorageSlowlogEnabled = tieredStorageSlowlogEnabled;
    }

    private void setQueryWarnThreshold(TimeValue warnThreshold) {
        this.queryWarnThreshold = warnThreshold.nanos();
    }

    private void setQueryInfoThreshold(TimeValue infoThreshold) {
        this.queryInfoThreshold = infoThreshold.nanos();
    }

    private void setQueryDebugThreshold(TimeValue debugThreshold) {
        this.queryDebugThreshold = debugThreshold.nanos();
    }

    private void setQueryTraceThreshold(TimeValue traceThreshold) {
        this.queryTraceThreshold = traceThreshold.nanos();
    }

    private void setFetchWarnThreshold(TimeValue warnThreshold) {
        this.fetchWarnThreshold = warnThreshold.nanos();
    }

    private void setFetchInfoThreshold(TimeValue infoThreshold) {
        this.fetchInfoThreshold = infoThreshold.nanos();
    }

    private void setFetchDebugThreshold(TimeValue debugThreshold) {
        this.fetchDebugThreshold = debugThreshold.nanos();
    }

    private void setFetchTraceThreshold(TimeValue traceThreshold) {
        this.fetchTraceThreshold = traceThreshold.nanos();
    }

    /**
     * Returns the query warn threshold in nanoseconds.
     * @return the threshold
     */
    long getQueryWarnThreshold() {
        return queryWarnThreshold;
    }

    /**
     * Returns the query info threshold in nanoseconds.
     * @return the threshold
     */
    long getQueryInfoThreshold() {
        return queryInfoThreshold;
    }

    /**
     * Returns the query debug threshold in nanoseconds.
     * @return the threshold
     */
    long getQueryDebugThreshold() {
        return queryDebugThreshold;
    }

    /**
     * Returns the query trace threshold in nanoseconds.
     * @return the threshold
     */
    long getQueryTraceThreshold() {
        return queryTraceThreshold;
    }

    /**
     * Returns the fetch warn threshold in nanoseconds.
     * @return the threshold
     */
    long getFetchWarnThreshold() {
        return fetchWarnThreshold;
    }

    /**
     * Returns the fetch info threshold in nanoseconds.
     * @return the threshold
     */
    long getFetchInfoThreshold() {
        return fetchInfoThreshold;
    }

    /**
     * Returns the fetch debug threshold in nanoseconds.
     * @return the threshold
     */
    long getFetchDebugThreshold() {
        return fetchDebugThreshold;
    }

    /**
     * Returns the fetch trace threshold in nanoseconds.
     * @return the threshold
     */
    long getFetchTraceThreshold() {
        return fetchTraceThreshold;
    }

    /**
     * Returns the current slow log level.
     * @return the slow log level
     */
    // TODO check this level
    SlowLogLevel getLevel() {
        return level;
    }
}

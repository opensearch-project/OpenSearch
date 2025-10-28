/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index;

import com.fasterxml.jackson.core.io.JsonStringEncoder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.logging.OpenSearchLogMessage;
import org.opensearch.common.logging.SlowLogLevel;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.tasks.Task;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * The search time slow log implementation
 *
 * @opensearch.internal
 */
public final class SearchSlowLog implements SearchOperationListener {
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private long queryWarnThreshold;
    private long queryInfoThreshold;
    private long queryDebugThreshold;
    private long queryTraceThreshold;

    private long fetchWarnThreshold;
    private long fetchInfoThreshold;
    private long fetchDebugThreshold;
    private long fetchTraceThreshold;

    private final Logger queryLogger;
    private final Logger fetchLogger;

    static final String INDEX_SEARCH_SLOWLOG_PREFIX = "index.search.slowlog";
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING = Setting.timeSetting(
        INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.query.warn",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING = Setting.timeSetting(
        INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.query.info",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING = Setting.timeSetting(
        INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.query.debug",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING = Setting.timeSetting(
        INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.query.trace",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING = Setting.timeSetting(
        INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.warn",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING = Setting.timeSetting(
        INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.info",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING = Setting.timeSetting(
        INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.debug",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING = Setting.timeSetting(
        INDEX_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.trace",
        TimeValue.timeValueNanos(-1),
        TimeValue.timeValueMillis(-1),
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<SlowLogLevel> INDEX_SEARCH_SLOWLOG_LEVEL = new Setting<>(
        INDEX_SEARCH_SLOWLOG_PREFIX + ".level",
        SlowLogLevel.TRACE.name(),
        SlowLogLevel::parse,
        Property.Dynamic,
        Property.IndexScope
    );

    private static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));
    private SlowLogLevel level;

    public SearchSlowLog(IndexSettings indexSettings) {
        this.queryLogger = LogManager.getLogger(INDEX_SEARCH_SLOWLOG_PREFIX + ".query");
        this.fetchLogger = LogManager.getLogger(INDEX_SEARCH_SLOWLOG_PREFIX + ".fetch");
        Loggers.setLevel(this.fetchLogger, SlowLogLevel.TRACE.name());
        Loggers.setLevel(this.queryLogger, SlowLogLevel.TRACE.name());

        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING, this::setQueryWarnThreshold);
        this.queryWarnThreshold = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING).nanos();
        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING, this::setQueryInfoThreshold);
        this.queryInfoThreshold = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING).nanos();
        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING, this::setQueryDebugThreshold);
        this.queryDebugThreshold = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING).nanos();
        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING, this::setQueryTraceThreshold);
        this.queryTraceThreshold = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING).nanos();

        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING, this::setFetchWarnThreshold);
        this.fetchWarnThreshold = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING).nanos();
        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING, this::setFetchInfoThreshold);
        this.fetchInfoThreshold = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING).nanos();
        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING, this::setFetchDebugThreshold);
        this.fetchDebugThreshold = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING).nanos();
        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING, this::setFetchTraceThreshold);
        this.fetchTraceThreshold = indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING).nanos();

        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_LEVEL, this::setLevel);
        setLevel(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_LEVEL));
    }

    private void setLevel(SlowLogLevel level) {
        this.level = level;
    }

    @Override
    public void onQueryPhase(SearchContext context, long tookInNanos) {
        if (queryWarnThreshold >= 0 && tookInNanos > queryWarnThreshold && level.isLevelEnabledFor(SlowLogLevel.WARN)) {
            queryLogger.warn(new SearchSlowLogMessage(context, tookInNanos));
        } else if (queryInfoThreshold >= 0 && tookInNanos > queryInfoThreshold && level.isLevelEnabledFor(SlowLogLevel.INFO)) {
            queryLogger.info(new SearchSlowLogMessage(context, tookInNanos));
        } else if (queryDebugThreshold >= 0 && tookInNanos > queryDebugThreshold && level.isLevelEnabledFor(SlowLogLevel.DEBUG)) {
            queryLogger.debug(new SearchSlowLogMessage(context, tookInNanos));
        } else if (queryTraceThreshold >= 0 && tookInNanos > queryTraceThreshold && level.isLevelEnabledFor(SlowLogLevel.TRACE)) {
            queryLogger.trace(new SearchSlowLogMessage(context, tookInNanos));
        }
    }

    @Override
    public void onFetchPhase(SearchContext context, long tookInNanos) {
        if (fetchWarnThreshold >= 0 && tookInNanos > fetchWarnThreshold && level.isLevelEnabledFor(SlowLogLevel.WARN)) {
            fetchLogger.warn(new SearchSlowLogMessage(context, tookInNanos));
        } else if (fetchInfoThreshold >= 0 && tookInNanos > fetchInfoThreshold && level.isLevelEnabledFor(SlowLogLevel.INFO)) {
            fetchLogger.info(new SearchSlowLogMessage(context, tookInNanos));
        } else if (fetchDebugThreshold >= 0 && tookInNanos > fetchDebugThreshold && level.isLevelEnabledFor(SlowLogLevel.DEBUG)) {
            fetchLogger.debug(new SearchSlowLogMessage(context, tookInNanos));
        } else if (fetchTraceThreshold >= 0 && tookInNanos > fetchTraceThreshold && level.isLevelEnabledFor(SlowLogLevel.TRACE)) {
            fetchLogger.trace(new SearchSlowLogMessage(context, tookInNanos));
        }
    }

    /**
     * Search slow log message
     *
     * @opensearch.internal
     */
    static final class SearchSlowLogMessage extends OpenSearchLogMessage {

        SearchSlowLogMessage(SearchContext context, long tookInNanos) {
            super(prepareMap(context, tookInNanos), message(context, tookInNanos));
        }

        private static Map<String, Object> prepareMap(SearchContext context, long tookInNanos) {
            Map<String, Object> messageFields = new HashMap<>();
            messageFields.put("message", context.indexShard().shardId());
            messageFields.put("took", TimeValue.timeValueNanos(tookInNanos));
            messageFields.put("took_millis", TimeUnit.NANOSECONDS.toMillis(tookInNanos));
            if (context.queryResult().getTotalHits() != null) {
                messageFields.put("total_hits", context.queryResult().getTotalHits());
            } else {
                messageFields.put("total_hits", "-1");
            }
            messageFields.put(
                "stats",
                escapeJson(asJsonArray(context.groupStats() != null ? context.groupStats().stream() : Stream.empty()))
            );
            messageFields.put("search_type", context.searchType());
            messageFields.put("total_shards", context.numberOfShards());

            if (context.request().source() != null) {
                String source = escapeJson(context.request().source().toString(FORMAT_PARAMS));

                messageFields.put("source", source);
            } else {
                messageFields.put("source", "{}");
            }

            messageFields.put("id", context.getTask().getHeader(Task.X_OPAQUE_ID));
            messageFields.put("trace-id", context.getTask().getHeader(Task.TRACE_ID));
            return messageFields;
        }

        // Message will be used in plaintext logs
        private static String message(SearchContext context, long tookInNanos) {
            StringBuilder sb = new StringBuilder();
            sb.append(context.indexShard().shardId())
                .append(" ")
                .append("took[")
                .append(TimeValue.timeValueNanos(tookInNanos))
                .append("], ")
                .append("took_millis[")
                .append(TimeUnit.NANOSECONDS.toMillis(tookInNanos))
                .append("], ")
                .append("total_hits[");
            if (context.queryResult().getTotalHits() != null) {
                sb.append(context.queryResult().getTotalHits());
            } else {
                sb.append("-1");
            }
            sb.append("], ");
            if (context.groupStats() == null) {
                sb.append("stats[], ");
            } else {
                sb.append("stats[");
                Strings.collectionToDelimitedString(context.groupStats(), ",", "", "", sb);
                sb.append("], ");
            }
            sb.append("search_type[")
                .append(context.searchType())
                .append("], total_shards[")
                .append(context.numberOfShards())
                .append("], ");
            if (context.request().source() != null) {
                sb.append("source[").append(context.request().source().toString(FORMAT_PARAMS)).append("], ");
            } else {
                sb.append("source[], ");
            }
            if (context.getTask().getHeader(Task.X_OPAQUE_ID) != null) {
                sb.append("id[").append(context.getTask().getHeader(Task.X_OPAQUE_ID)).append("], ");
            } else {
                sb.append("id[], ");
            }
            if (context.getTask().getHeader(Task.TRACE_ID) != null) {
                sb.append("trace-id[").append(context.getTask().getHeader(Task.TRACE_ID)).append("]");
            } else {
                sb.append("trace-id[], ");
            }
            return sb.toString();
        }

        private static String escapeJson(String text) {
            byte[] sourceEscaped = JsonStringEncoder.getInstance().quoteAsUTF8(text);
            return new String(sourceEscaped, UTF_8);
        }
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

    long getQueryWarnThreshold() {
        return queryWarnThreshold;
    }

    long getQueryInfoThreshold() {
        return queryInfoThreshold;
    }

    long getQueryDebugThreshold() {
        return queryDebugThreshold;
    }

    long getQueryTraceThreshold() {
        return queryTraceThreshold;
    }

    long getFetchWarnThreshold() {
        return fetchWarnThreshold;
    }

    long getFetchInfoThreshold() {
        return fetchInfoThreshold;
    }

    long getFetchDebugThreshold() {
        return fetchDebugThreshold;
    }

    long getFetchTraceThreshold() {
        return fetchTraceThreshold;
    }

    SlowLogLevel getLevel() {
        return level;
    }
}

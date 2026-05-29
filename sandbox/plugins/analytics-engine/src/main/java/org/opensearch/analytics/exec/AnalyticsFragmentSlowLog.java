/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.AnalyticsOperationListener;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.logging.SlowLogLevel;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.SearchSlowLog;

import java.util.concurrent.TimeUnit;

/**
 * Data-node-level slow log for analytics engine fragment executions.
 * Reads per-index thresholds from {@link IndexSettings} passed at call time,
 * mirroring how {@link SearchSlowLog} uses per-index settings for shard-level
 * query timing in the standard search path.
 */
public class AnalyticsFragmentSlowLog implements AnalyticsOperationListener {

    private final Logger logger;

    public static final String LOGGER_NAME = "index.search.slowlog";

    public AnalyticsFragmentSlowLog() {
        this.logger = LogManager.getLogger(LOGGER_NAME);
        Loggers.setLevel(logger, SlowLogLevel.TRACE.name());
    }

    @Override
    public void onFragmentSuccess(
        String queryId,
        int stageId,
        String shardId,
        long tookInNanos,
        long rowsProduced,
        IndexSettings indexSettings
    ) {
        long warnThreshold = indexSettings.getValue(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING).nanos();
        long infoThreshold = indexSettings.getValue(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING).nanos();
        long debugThreshold = indexSettings.getValue(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING).nanos();
        long traceThreshold = indexSettings.getValue(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING).nanos();
        SlowLogLevel level = indexSettings.getValue(SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL);

        String message = formatMessage(queryId, stageId, shardId, tookInNanos, rowsProduced);
        if (warnThreshold >= 0 && tookInNanos > warnThreshold && level.isLevelEnabledFor(SlowLogLevel.WARN)) {
            logger.warn(message);
        } else if (infoThreshold >= 0 && tookInNanos > infoThreshold && level.isLevelEnabledFor(SlowLogLevel.INFO)) {
            logger.info(message);
        } else if (debugThreshold >= 0 && tookInNanos > debugThreshold && level.isLevelEnabledFor(SlowLogLevel.DEBUG)) {
            logger.debug(message);
        } else if (traceThreshold >= 0 && tookInNanos > traceThreshold && level.isLevelEnabledFor(SlowLogLevel.TRACE)) {
            logger.trace(message);
        }
    }

    private static String formatMessage(String queryId, int stageId, String shardId, long tookInNanos, long rowsProduced) {
        return "took["
            + TimeValue.timeValueNanos(tookInNanos)
            + "], took_millis["
            + TimeUnit.NANOSECONDS.toMillis(tookInNanos)
            + "], query_id["
            + queryId
            + "], stage_id["
            + stageId
            + "], shard["
            + shardId
            + "], rows_produced["
            + rowsProduced
            + "]";
    }
}

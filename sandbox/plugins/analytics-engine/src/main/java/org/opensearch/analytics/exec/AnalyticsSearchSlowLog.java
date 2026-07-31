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
import org.opensearch.action.search.SearchRequestSlowLog;
import org.opensearch.analytics.backend.AnalyticsOperationListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.logging.SlowLogLevel;
import org.opensearch.common.unit.TimeValue;

import java.util.concurrent.TimeUnit;

/**
 * Coordinator-level slow log for analytics engine queries. Logs at {@code onQueryComplete}
 * — the terminal event fired after planning, execution, and materialization are all done.
 *
 * <p>Reuses the existing {@code cluster.search.request.slowlog.*} settings so operators
 * have a single configuration surface for both standard and analytics search paths.
 *
 * <p>{@link #createQueryListener(String)} creates a per-query listener that observes the
 * full query lifecycle — planning, stage execution, and completion — accumulating timing
 * data and emitting the slow log entry at {@code onQueryComplete}.
 */
public class AnalyticsSearchSlowLog implements AnalyticsOperationListener {

    private volatile long warnThreshold;
    private volatile long infoThreshold;
    private volatile long debugThreshold;
    private volatile long traceThreshold;
    private volatile SlowLogLevel level;

    private final Logger queryLogger;

    public static final String QUERY_LOGGER_NAME = "cluster.search.request.slowlog";

    public AnalyticsSearchSlowLog(ClusterService clusterService) {
        this.queryLogger = LogManager.getLogger(QUERY_LOGGER_NAME);
        Loggers.setLevel(queryLogger, SlowLogLevel.TRACE.name());

        this.warnThreshold = clusterService.getClusterSettings()
            .get(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_WARN_SETTING)
            .nanos();
        this.infoThreshold = clusterService.getClusterSettings()
            .get(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_INFO_SETTING)
            .nanos();
        this.debugThreshold = clusterService.getClusterSettings()
            .get(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_DEBUG_SETTING)
            .nanos();
        this.traceThreshold = clusterService.getClusterSettings()
            .get(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_TRACE_SETTING)
            .nanos();
        this.level = clusterService.getClusterSettings().get(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_LEVEL);

        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_WARN_SETTING,
                t -> warnThreshold = t.nanos()
            );
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_INFO_SETTING,
                t -> infoThreshold = t.nanos()
            );
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_DEBUG_SETTING,
                t -> debugThreshold = t.nanos()
            );
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_TRACE_SETTING,
                t -> traceThreshold = t.nanos()
            );
        clusterService.getClusterSettings().addSettingsUpdateConsumer(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_LEVEL, l -> {
            this.level = l;
            Loggers.setLevel(queryLogger, l.name());
        });
    }

    /**
     * Creates a per-query listener that observes the full query lifecycle. Must be created
     * before planning starts so it can capture planning time, stage timing, and emit the
     * slow log at query completion. Task headers are set later via {@link QuerySlowLogListener#setHeaders}.
     */
    public QuerySlowLogListener createQueryListener(String querySource) {
        return new QuerySlowLogListener(querySource);
    }

    private void logAtLevel(String message, long tookInNanos) {
        if (warnThreshold >= 0 && tookInNanos > warnThreshold && level.isLevelEnabledFor(SlowLogLevel.WARN)) {
            queryLogger.warn(message);
        } else if (infoThreshold >= 0 && tookInNanos > infoThreshold && level.isLevelEnabledFor(SlowLogLevel.INFO)) {
            queryLogger.info(message);
        } else if (debugThreshold >= 0 && tookInNanos > debugThreshold && level.isLevelEnabledFor(SlowLogLevel.DEBUG)) {
            queryLogger.debug(message);
        } else if (traceThreshold >= 0 && tookInNanos > traceThreshold && level.isLevelEnabledFor(SlowLogLevel.TRACE)) {
            queryLogger.trace(message);
        }
    }

    /**
     * Per-query listener that accumulates planning time, stage timing, and task headers,
     * then emits the complete slow log entry at {@code onQueryComplete}.
     */
    class QuerySlowLogListener implements AnalyticsOperationListener {
        private final String querySource;
        private volatile String opaqueId;
        private volatile String requestId;
        private long planningTimeMs;
        private final StringBuilder stageTook = new StringBuilder();

        QuerySlowLogListener(String querySource) {
            this.querySource = querySource;
        }

        void setHeaders(String opaqueId, String requestId) {
            this.opaqueId = opaqueId;
            this.requestId = requestId;
        }

        @Override
        public void onPlanningComplete(String queryId, long tookInNanos) {
            this.planningTimeMs = TimeUnit.NANOSECONDS.toMillis(tookInNanos);
        }

        @Override
        public void onStageSuccess(String queryId, int stageId, String stageType, long tookInNanos, long rowsProcessed) {
            if (stageTook.length() > 0) stageTook.append(", ");
            stageTook.append(stageType).append("=").append(TimeUnit.NANOSECONDS.toMillis(tookInNanos));
        }

        @Override
        public void onQueryComplete(String queryId, long totalTookInNanos, long totalRows) {
            StringBuilder sb = new StringBuilder();
            sb.append("took[").append(TimeValue.timeValueNanos(totalTookInNanos)).append("], ");
            sb.append("took_millis[").append(TimeUnit.NANOSECONDS.toMillis(totalTookInNanos)).append("], ");
            sb.append("planning_time_millis[").append(planningTimeMs).append("], ");
            sb.append("stage_took_millis[{").append(stageTook).append("}], ");
            sb.append("query_id[").append(queryId).append("], ");
            sb.append("total_rows[").append(totalRows).append("], ");
            sb.append("source[").append(querySource != null ? querySource : "").append("], ");
            sb.append("id[").append(opaqueId != null ? opaqueId : "").append("], ");
            sb.append("request_id[").append(requestId != null ? requestId : "").append("]");
            logAtLevel(sb.toString(), totalTookInNanos);
        }
    }
}

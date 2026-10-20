/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequestSlowLog;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnalyticsSearchSlowLogTests extends OpenSearchTestCase {

    private AnalyticsSearchSlowLog createSlowLog(TimeValue warnThreshold) {
        Settings settings = Settings.builder()
            .put(SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_WARN_SETTING.getKey(), warnThreshold)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(
                SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_WARN_SETTING,
                SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_INFO_SETTING,
                SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_DEBUG_SETTING,
                SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_THRESHOLD_TRACE_SETTING,
                SearchRequestSlowLog.CLUSTER_SEARCH_REQUEST_SLOWLOG_LEVEL
            )
        );
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.getSettings()).thenReturn(settings);
        return new AnalyticsSearchSlowLog(clusterService);
    }

    public void testSlowLogFiresAtOnQueryComplete() throws Exception {
        AnalyticsSearchSlowLog slowLog = createSlowLog(TimeValue.timeValueMillis(0));
        Logger logger = LogManager.getLogger(AnalyticsSearchSlowLog.QUERY_LOGGER_NAME);
        Loggers.setLevel(logger, Level.WARN);

        var wrapped = slowLog.createQueryListener("SELECT * FROM idx");
        wrapped.setHeaders("opaque-1", "req-1");

        try (MockLogAppender appender = MockLogAppender.createForLoggers(logger)) {
            appender.addExpectation(
                new MockLogAppender.PatternSeenWithLoggerPrefixExpectation(
                    "slow log fires at query complete",
                    AnalyticsSearchSlowLog.QUERY_LOGGER_NAME,
                    Level.WARN,
                    ".*took\\[.*\\].*query_id\\[q1\\].*total_rows\\[50\\].*"
                )
            );

            wrapped.onQueryComplete("q1", TimeValue.timeValueMillis(10).nanos(), 50);
            appender.assertAllExpectationsMatched();
        }
    }

    public void testSlowLogDoesNotFireWhenBelowThreshold() throws Exception {
        AnalyticsSearchSlowLog slowLog = createSlowLog(TimeValue.timeValueSeconds(10));
        Logger logger = LogManager.getLogger(AnalyticsSearchSlowLog.QUERY_LOGGER_NAME);
        Loggers.setLevel(logger, Level.WARN);

        var wrapped = slowLog.createQueryListener("SELECT 1");

        try (MockLogAppender appender = MockLogAppender.createForLoggers(logger)) {
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "no log below threshold",
                    AnalyticsSearchSlowLog.QUERY_LOGGER_NAME,
                    Level.WARN,
                    "*"
                )
            );

            wrapped.onQueryComplete("q2", TimeValue.timeValueMillis(5).nanos(), 10);
            appender.assertAllExpectationsMatched();
        }
    }

    public void testSlowLogIncludesPlanningAndStageTiming() throws Exception {
        AnalyticsSearchSlowLog slowLog = createSlowLog(TimeValue.timeValueMillis(0));
        Logger logger = LogManager.getLogger(AnalyticsSearchSlowLog.QUERY_LOGGER_NAME);
        Loggers.setLevel(logger, Level.WARN);

        var wrapped = slowLog.createQueryListener("SELECT val FROM idx");

        try (MockLogAppender appender = MockLogAppender.createForLoggers(logger)) {
            appender.addExpectation(
                new MockLogAppender.PatternSeenWithLoggerPrefixExpectation(
                    "planning and stage timing present",
                    AnalyticsSearchSlowLog.QUERY_LOGGER_NAME,
                    Level.WARN,
                    ".*planning_time_millis\\[2\\].*stage_took_millis\\[\\{ShardFragmentStageExecution=8, ReduceStageExecution=3\\}\\].*"
                )
            );

            wrapped.onPlanningComplete("q3", TimeValue.timeValueMillis(2).nanos());
            wrapped.onStageSuccess("q3", 0, "ShardFragmentStageExecution", TimeValue.timeValueMillis(8).nanos(), 100);
            wrapped.onStageSuccess("q3", 1, "ReduceStageExecution", TimeValue.timeValueMillis(3).nanos(), 100);
            wrapped.onQueryComplete("q3", TimeValue.timeValueMillis(14).nanos(), 100);

            appender.assertAllExpectationsMatched();
        }
    }

    public void testSlowLogIncludesSourceAndHeaders() throws Exception {
        AnalyticsSearchSlowLog slowLog = createSlowLog(TimeValue.timeValueMillis(0));
        Logger logger = LogManager.getLogger(AnalyticsSearchSlowLog.QUERY_LOGGER_NAME);
        Loggers.setLevel(logger, Level.WARN);

        var wrapped = slowLog.createQueryListener("source = my_index | stats count()");
        wrapped.setHeaders("user-opaque-123", "req-456");

        try (MockLogAppender appender = MockLogAppender.createForLoggers(logger)) {
            appender.addExpectation(
                new MockLogAppender.PatternSeenWithLoggerPrefixExpectation(
                    "source in log",
                    AnalyticsSearchSlowLog.QUERY_LOGGER_NAME,
                    Level.WARN,
                    ".*source\\[source = my_index \\| stats count\\(\\)\\].*id\\[user-opaque-123\\].*request_id\\[req-456\\].*"
                )
            );

            wrapped.onQueryComplete("q4", TimeValue.timeValueMillis(10).nanos(), 1);
            appender.assertAllExpectationsMatched();
        }
    }

    public void testSlowLogWithNullMetadata() throws Exception {
        AnalyticsSearchSlowLog slowLog = createSlowLog(TimeValue.timeValueMillis(0));
        Logger logger = LogManager.getLogger(AnalyticsSearchSlowLog.QUERY_LOGGER_NAME);
        Loggers.setLevel(logger, Level.WARN);

        var wrapped = slowLog.createQueryListener(null);

        try (MockLogAppender appender = MockLogAppender.createForLoggers(logger)) {
            appender.addExpectation(
                new MockLogAppender.PatternSeenWithLoggerPrefixExpectation(
                    "empty fields present",
                    AnalyticsSearchSlowLog.QUERY_LOGGER_NAME,
                    Level.WARN,
                    ".*source\\[\\].*id\\[\\].*request_id\\[\\].*"
                )
            );

            wrapped.onQueryComplete("q5", TimeValue.timeValueMillis(1).nanos(), 0);
            appender.assertAllExpectationsMatched();
        }
    }
}

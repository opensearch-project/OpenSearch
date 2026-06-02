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
import org.opensearch.Version;
import org.opensearch.analytics.backend.FragmentExecutionStats;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.SearchSlowLog;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

public class AnalyticsFragmentSlowLogTests extends OpenSearchTestCase {

    private IndexSettings createIndexSettings(TimeValue warnThreshold) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_UUID, "test-uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING.getKey(), warnThreshold)
            .build();
        IndexMetadata metadata = IndexMetadata.builder("test-index").settings(settings).build();
        return new IndexSettings(metadata, Settings.EMPTY);
    }

    public void testFragmentSlowLogFiresWhenAboveThreshold() throws Exception {
        AnalyticsFragmentSlowLog fragmentSlowLog = new AnalyticsFragmentSlowLog();
        Logger logger = LogManager.getLogger(AnalyticsFragmentSlowLog.LOGGER_NAME);
        Loggers.setLevel(logger, Level.WARN);

        IndexSettings indexSettings = createIndexSettings(TimeValue.timeValueMillis(0));

        try (MockLogAppender appender = MockLogAppender.createForLoggers(logger)) {
            appender.addExpectation(
                new MockLogAppender.PatternSeenWithLoggerPrefixExpectation(
                    "fragment log fires",
                    AnalyticsFragmentSlowLog.LOGGER_NAME,
                    Level.WARN,
                    ".*shard\\[test-shard\\].*rows_produced\\[42\\].*"
                )
            );

            fragmentSlowLog.onFragmentSuccess(
                "q1",
                0,
                "test-shard",
                TimeValue.timeValueMillis(5).nanos(),
                indexSettings,
                new FragmentExecutionStats(42, true, 2, "CONJUNCTIVE", false, 99, "opaque-1")
            );
            appender.assertAllExpectationsMatched();
        }
    }

    public void testFragmentSlowLogDoesNotFireWhenBelowThreshold() throws Exception {
        AnalyticsFragmentSlowLog fragmentSlowLog = new AnalyticsFragmentSlowLog();
        Logger logger = LogManager.getLogger(AnalyticsFragmentSlowLog.LOGGER_NAME);
        Loggers.setLevel(logger, Level.WARN);

        IndexSettings indexSettings = createIndexSettings(TimeValue.timeValueMinutes(10));

        try (MockLogAppender appender = MockLogAppender.createForLoggers(logger)) {
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "no fragment log below threshold",
                    AnalyticsFragmentSlowLog.LOGGER_NAME,
                    Level.WARN,
                    "*"
                )
            );

            fragmentSlowLog.onFragmentSuccess(
                "q1",
                0,
                "test-shard",
                TimeValue.timeValueMillis(5).nanos(),
                indexSettings,
                new FragmentExecutionStats(42, true, 2, "CONJUNCTIVE", false, 99, "opaque-1")
            );
            appender.assertAllExpectationsMatched();
        }
    }

    public void testFragmentSlowLogContainsAllExpectedFields() throws Exception {
        AnalyticsFragmentSlowLog fragmentSlowLog = new AnalyticsFragmentSlowLog();
        Logger logger = LogManager.getLogger(AnalyticsFragmentSlowLog.LOGGER_NAME);
        Loggers.setLevel(logger, Level.WARN);

        IndexSettings indexSettings = createIndexSettings(TimeValue.timeValueMillis(0));

        try (MockLogAppender appender = MockLogAppender.createForLoggers(logger)) {
            appender.addExpectation(
                new MockLogAppender.PatternSeenWithLoggerPrefixExpectation(
                    "all fields present",
                    AnalyticsFragmentSlowLog.LOGGER_NAME,
                    Level.WARN,
                    ".*took\\[.*\\].*took_millis\\[\\d+\\].*query_id\\[q-fields\\].*stage_id\\[2\\].*shard\\[\\[my-idx\\]\\[0\\]\\].*rows_produced\\[100\\].*used_secondary_index\\[false\\].*partial_aggregate\\[true\\].*task_id\\[101\\].*id\\[opaque-2\\].*"
                )
            );

            fragmentSlowLog.onFragmentSuccess(
                "q-fields",
                2,
                "[my-idx][0]",
                TimeValue.timeValueMillis(50).nanos(),
                indexSettings,
                new FragmentExecutionStats(100, false, 0, null, true, 101, "opaque-2")
            );
            appender.assertAllExpectationsMatched();
        }
    }

    public void testFragmentSlowLogWithSecondaryIndexShowsDelegationFields() throws Exception {
        AnalyticsFragmentSlowLog fragmentSlowLog = new AnalyticsFragmentSlowLog();
        Logger logger = LogManager.getLogger(AnalyticsFragmentSlowLog.LOGGER_NAME);
        Loggers.setLevel(logger, Level.WARN);

        IndexSettings indexSettings = createIndexSettings(TimeValue.timeValueMillis(0));

        try (MockLogAppender appender = MockLogAppender.createForLoggers(logger)) {
            appender.addExpectation(
                new MockLogAppender.PatternSeenWithLoggerPrefixExpectation(
                    "delegation fields present when secondary index used",
                    AnalyticsFragmentSlowLog.LOGGER_NAME,
                    Level.WARN,
                    ".*used_secondary_index\\[true\\].*delegated_predicates\\[3\\].*filter_tree_shape\\[CONJUNCTIVE\\].*"
                )
            );

            fragmentSlowLog.onFragmentSuccess(
                "q-sec",
                0,
                "[idx][0]",
                TimeValue.timeValueMillis(10).nanos(),
                indexSettings,
                new FragmentExecutionStats(50, true, 3, "CONJUNCTIVE", false, 200, null)
            );
            appender.assertAllExpectationsMatched();
        }
    }

    public void testFragmentSlowLogWithoutSecondaryIndexOmitsDelegationFields() throws Exception {
        AnalyticsFragmentSlowLog fragmentSlowLog = new AnalyticsFragmentSlowLog();
        Logger logger = LogManager.getLogger(AnalyticsFragmentSlowLog.LOGGER_NAME);
        Loggers.setLevel(logger, Level.WARN);

        IndexSettings indexSettings = createIndexSettings(TimeValue.timeValueMillis(0));

        try (MockLogAppender appender = MockLogAppender.createForLoggers(logger)) {
            appender.addExpectation(
                new MockLogAppender.PatternSeenWithLoggerPrefixExpectation(
                    "no delegation fields when secondary index not used",
                    AnalyticsFragmentSlowLog.LOGGER_NAME,
                    Level.WARN,
                    ".*used_secondary_index\\[false\\].*partial_aggregate\\[.*"
                )
            );
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "delegated_predicates absent",
                    AnalyticsFragmentSlowLog.LOGGER_NAME,
                    Level.WARN,
                    "*delegated_predicates*"
                )
            );

            fragmentSlowLog.onFragmentSuccess(
                "q-nosec",
                0,
                "[idx][0]",
                TimeValue.timeValueMillis(10).nanos(),
                indexSettings,
                new FragmentExecutionStats(50, false, 0, null, false, 201, "op-1")
            );
            appender.assertAllExpectationsMatched();
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexingSlowLog;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchIntegTestCase;

/**
 * Verifies that the indexing slow log fires with full fidelity when documents
 * are indexed through the DataFormatAware (Parquet) engine.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeIndexingSlowLogIT extends AbstractCompositeEngineIT {

    private static final String INDEX_NAME = "slowlog-test-idx";
    private static final String SLOW_LOG_LOGGER = IndexingSlowLog.INDEX_INDEXING_SLOWLOG_PREFIX + ".index";

    public void testSlowLogEmitsAllFieldsForDFAEngine() throws Exception {
        createIndexWithSlowLogThreshold(TimeValue.timeValueMillis(0));

        try (MockLogAppender appender = newSlowLogAppender()) {
            appender.addExpectation(expectSeen("contains index name", ".*slowlog-test-idx.*"));
            appender.addExpectation(expectSeen("contains took", ".*took\\[.*\\].*"));
            appender.addExpectation(expectSeen("contains took_millis", ".*took_millis\\[\\d+\\].*"));
            appender.addExpectation(expectSeen("contains document id", ".*id\\[.*\\].*"));
            appender.addExpectation(expectSeen("contains routing field", ".*routing\\[.*\\].*"));
            appender.addExpectation(expectSeen("contains document source", ".*source\\[.*test_value.*\\].*"));

            assertEquals(
                RestStatus.CREATED,
                client().prepareIndex().setIndex(INDEX_NAME).setSource("name", "test_value", "value", 42).get().status()
            );

            appender.assertAllExpectationsMatched();
        }
    }

    public void testSlowLogDoesNotFireWhenBelowThreshold() throws Exception {
        createIndexWithSlowLogThreshold(TimeValue.timeValueMinutes(10));

        try (MockLogAppender appender = newSlowLogAppender()) {
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation("no slow log below threshold", SLOW_LOG_LOGGER, Level.WARN, "*")
            );

            assertEquals(
                RestStatus.CREATED,
                client().prepareIndex().setIndex(INDEX_NAME).setSource("name", "fast_doc", "value", 1).get().status()
            );

            appender.assertAllExpectationsMatched();
        }
    }

    private void createIndexWithSlowLogThreshold(TimeValue threshold) {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.pluggable.dataformat.enabled", true)
                    .put("index.pluggable.dataformat", "composite")
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats", "lucene")
                    .put(IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING.getKey(), threshold)
            )
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);
    }

    private MockLogAppender newSlowLogAppender() throws IllegalAccessException {
        Logger logger = LogManager.getLogger(SLOW_LOG_LOGGER);
        Loggers.setLevel(logger, Level.WARN);
        return MockLogAppender.createForLoggers(logger);
    }

    private static MockLogAppender.PatternSeenWithLoggerPrefixExpectation expectSeen(String name, String regex) {
        return new MockLogAppender.PatternSeenWithLoggerPrefixExpectation(name, SLOW_LOG_LOGGER, Level.WARN, regex);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporterFactory;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

/**
 * Unit Tests for {@link QueryInsightsService}.
 */
public class TopQueriesServiceTests extends OpenSearchTestCase {
    private TopQueriesService topQueriesService;
    private final ThreadPool threadPool = mock(ThreadPool.class);
    private final QueryInsightsExporterFactory queryInsightsExporterFactory = mock(QueryInsightsExporterFactory.class);

    @Before
    public void setup() {
        topQueriesService = new TopQueriesService(MetricType.LATENCY, threadPool, queryInsightsExporterFactory);
        topQueriesService.setTopNSize(Integer.MAX_VALUE);
        topQueriesService.setWindowSize(new TimeValue(Long.MAX_VALUE));
        topQueriesService.setEnabled(true);
    }

    public void testIngestQueryDataWithLargeWindow() {
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);
        topQueriesService.consumeRecords(records);
        assertTrue(
            QueryInsightsTestUtils.checkRecordsEqualsWithoutOrder(
                topQueriesService.getTopQueriesRecords(false),
                records,
                MetricType.LATENCY
            )
        );
    }

    public void testRollingWindows() {
        List<SearchQueryRecord> records;
        // Create 5 records at Now - 10 minutes to make sure they belong to the last window
        records = QueryInsightsTestUtils.generateQueryInsightRecords(5, 5, System.currentTimeMillis() - 1000 * 60 * 10, 0);
        topQueriesService.setWindowSize(TimeValue.timeValueMinutes(10));
        topQueriesService.consumeRecords(records);
        assertEquals(0, topQueriesService.getTopQueriesRecords(true).size());

        // Create 10 records at now + 1 minute, to make sure they belong to the current window
        records = QueryInsightsTestUtils.generateQueryInsightRecords(10, 10, System.currentTimeMillis() + 1000 * 60, 0);
        topQueriesService.setWindowSize(TimeValue.timeValueMinutes(10));
        topQueriesService.consumeRecords(records);
        assertEquals(10, topQueriesService.getTopQueriesRecords(true).size());
    }

    public void testSmallNSize() {
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);
        topQueriesService.setTopNSize(1);
        topQueriesService.consumeRecords(records);
        assertEquals(1, topQueriesService.getTopQueriesRecords(false).size());
    }

    public void testValidateTopNSize() {
        assertThrows(IllegalArgumentException.class, () -> { topQueriesService.validateTopNSize(QueryInsightsSettings.MAX_N_SIZE + 1); });
    }

    public void testValidateNegativeTopNSize() {
        assertThrows(IllegalArgumentException.class, () -> { topQueriesService.validateTopNSize(-1); });
    }

    public void testGetTopQueriesWhenNotEnabled() {
        topQueriesService.setEnabled(false);
        assertThrows(IllegalArgumentException.class, () -> { topQueriesService.getTopQueriesRecords(false); });
    }

    public void testValidateWindowSize() {
        assertThrows(IllegalArgumentException.class, () -> {
            topQueriesService.validateWindowSize(new TimeValue(QueryInsightsSettings.MAX_WINDOW_SIZE.getSeconds() + 1, TimeUnit.SECONDS));
        });
        assertThrows(IllegalArgumentException.class, () -> {
            topQueriesService.validateWindowSize(new TimeValue(QueryInsightsSettings.MIN_WINDOW_SIZE.getSeconds() - 1, TimeUnit.SECONDS));
        });
        assertThrows(IllegalArgumentException.class, () -> { topQueriesService.validateWindowSize(new TimeValue(2, TimeUnit.DAYS)); });
        assertThrows(IllegalArgumentException.class, () -> { topQueriesService.validateWindowSize(new TimeValue(7, TimeUnit.MINUTES)); });
    }

    private static void runUntilTimeoutOrFinish(DeterministicTaskQueue deterministicTaskQueue, long duration) {
        final long endTime = deterministicTaskQueue.getCurrentTimeMillis() + duration;
        while (deterministicTaskQueue.getCurrentTimeMillis() < endTime
            && (deterministicTaskQueue.hasRunnableTasks() || deterministicTaskQueue.hasDeferredTasks())) {
            if (deterministicTaskQueue.hasDeferredTasks() && randomBoolean()) {
                deterministicTaskQueue.advanceTime();
            } else if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            }
        }
    }
}

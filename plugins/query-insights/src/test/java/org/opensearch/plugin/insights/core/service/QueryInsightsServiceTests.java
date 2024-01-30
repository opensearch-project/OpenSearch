/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.node.Node;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Unit Tests for {@link QueryInsightsService}.
 */
public class QueryInsightsServiceTests extends OpenSearchTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;
    private ThreadPool threadPool;
    private QueryInsightsService queryInsightsService;

    @Before
    public void setup() {
        final Settings settings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "top n queries tests").build();
        deterministicTaskQueue = new DeterministicTaskQueue(settings, random());
        threadPool = deterministicTaskQueue.getThreadPool();
        queryInsightsService = new QueryInsightsService(threadPool);
        queryInsightsService.enableCollection(MetricType.LATENCY, true);
        queryInsightsService.enableCollection(MetricType.CPU, true);
        queryInsightsService.enableCollection(MetricType.JVM, true);
        queryInsightsService.setTopNSize(Integer.MAX_VALUE);
        queryInsightsService.setWindowSize(new TimeValue(Long.MAX_VALUE));
    }

    public void testIngestQueryDataWithLargeWindow() {
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);
        for (SearchQueryRecord record : records) {
            queryInsightsService.addRecord(record);
        }
        runUntilTimeoutOrFinish(deterministicTaskQueue, 5000);
        assertTrue(
            QueryInsightsTestUtils.checkRecordsEqualsWithoutOrder(
                queryInsightsService.getTopNRecords(MetricType.LATENCY, false),
                records,
                MetricType.LATENCY
            )
        );
    }

    public void testConcurrentIngestQueryDataWithLargeWindow() {
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);

        int numRequests = records.size();
        for (int i = 0; i < numRequests; i++) {
            int finalI = i;
            threadPool.schedule(() -> { queryInsightsService.addRecord(records.get(finalI)); }, TimeValue.ZERO, ThreadPool.Names.GENERIC);
        }
        runUntilTimeoutOrFinish(deterministicTaskQueue, 5000);
        assertTrue(
            QueryInsightsTestUtils.checkRecordsEqualsWithoutOrder(
                queryInsightsService.getTopNRecords(MetricType.LATENCY, false),
                records,
                MetricType.LATENCY
            )
        );
    }

    public void testRollingWindow() {
        // Create records with starting timestamp Monday, January 1, 2024 8:13:23 PM, with interval 10 minutes
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10, 10, 1704140003000L, 1000 * 60 * 10);
        queryInsightsService.setWindowSize(TimeValue.timeValueMinutes(10));
        queryInsightsService.addRecord(records.get(0));
        runUntilTimeoutOrFinish(deterministicTaskQueue, 5000);
        assertEquals(1, queryInsightsService.getTopNStoreSize(MetricType.LATENCY));
        assertEquals(0, queryInsightsService.getTopNHistoryStoreSize(MetricType.LATENCY));
        for (SearchQueryRecord record : records.subList(1, records.size())) {
            queryInsightsService.addRecord(record);
            runUntilTimeoutOrFinish(deterministicTaskQueue, 5000);
            assertEquals(1, queryInsightsService.getTopNStoreSize(MetricType.LATENCY));
            assertEquals(1, queryInsightsService.getTopNHistoryStoreSize(MetricType.LATENCY));
        }
        assertEquals(0, queryInsightsService.getTopNRecords(MetricType.LATENCY, false).size());
    }

    public void testRollingWindowWithHistory() {
        // Create 2 records with starting Now and last 10 minutes
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(
            2,
            2,
            System.currentTimeMillis() - 1000 * 60 * 10,
            1000 * 60 * 10
        );
        queryInsightsService.setWindowSize(TimeValue.timeValueMinutes(3));
        queryInsightsService.addRecord(records.get(0));
        runUntilTimeoutOrFinish(deterministicTaskQueue, 5000);
        assertEquals(1, queryInsightsService.getTopNStoreSize(MetricType.LATENCY));
        assertEquals(0, queryInsightsService.getTopNHistoryStoreSize(MetricType.LATENCY));
        queryInsightsService.addRecord(records.get(1));
        runUntilTimeoutOrFinish(deterministicTaskQueue, 5000);
        assertEquals(1, queryInsightsService.getTopNStoreSize(MetricType.LATENCY));
        assertEquals(0, queryInsightsService.getTopNHistoryStoreSize(MetricType.LATENCY));
        assertEquals(1, queryInsightsService.getTopNRecords(MetricType.LATENCY, true).size());
    }

    public void testSmallWindowClearOutdatedData() {
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(
            2,
            2,
            System.currentTimeMillis(),
            1000 * 60 * 20
        );
        queryInsightsService.setWindowSize(TimeValue.timeValueMinutes(10));

        for (SearchQueryRecord record : records) {
            queryInsightsService.addRecord(record);
        }
        runUntilTimeoutOrFinish(deterministicTaskQueue, 5000);
        assertTrue(queryInsightsService.getTopNRecords(MetricType.LATENCY, false).size() <= 1);
    }

    public void testSmallNSize() {
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);
        queryInsightsService.setTopNSize(1);

        for (SearchQueryRecord record : records) {
            queryInsightsService.addRecord(record);
        }
        runUntilTimeoutOrFinish(deterministicTaskQueue, 5000);
        assertEquals(1, queryInsightsService.getTopNRecords(MetricType.LATENCY, false).size());
    }

    public void testSmallNSizeWithCPU() {
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);
        queryInsightsService.setTopNSize(1);

        for (SearchQueryRecord record : records) {
            queryInsightsService.addRecord(record);
        }
        runUntilTimeoutOrFinish(deterministicTaskQueue, 5000);
        assertEquals(1, queryInsightsService.getTopNRecords(MetricType.CPU, false).size());
    }

    public void testSmallNSizeWithJVM() {
        final List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);
        queryInsightsService.setTopNSize(1);

        for (SearchQueryRecord record : records) {
            queryInsightsService.addRecord(record);
        }
        runUntilTimeoutOrFinish(deterministicTaskQueue, 5000);
        assertEquals(1, queryInsightsService.getTopNRecords(MetricType.JVM, false).size());
    }

    public void testValidateTopNSize() {
        assertThrows(
            IllegalArgumentException.class,
            () -> { queryInsightsService.validateTopNSize(QueryInsightsSettings.MAX_N_SIZE + 1); }
        );
    }

    public void testGetTopQueriesWhenNotEnabled() {
        queryInsightsService.enableCollection(MetricType.LATENCY, false);
        assertThrows(IllegalArgumentException.class, () -> { queryInsightsService.getTopNRecords(MetricType.LATENCY, false); });
    }

    public void testValidateWindowSize() {
        assertThrows(IllegalArgumentException.class, () -> {
            queryInsightsService.validateWindowSize(
                new TimeValue(QueryInsightsSettings.MAX_WINDOW_SIZE.getSeconds() + 1, TimeUnit.SECONDS)
            );
        });
        assertThrows(IllegalArgumentException.class, () -> {
            queryInsightsService.validateWindowSize(
                new TimeValue(QueryInsightsSettings.MIN_WINDOW_SIZE.getSeconds() - 1, TimeUnit.SECONDS)
            );
        });
        assertThrows(IllegalArgumentException.class, () -> { queryInsightsService.validateWindowSize(new TimeValue(2, TimeUnit.DAYS)); });
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

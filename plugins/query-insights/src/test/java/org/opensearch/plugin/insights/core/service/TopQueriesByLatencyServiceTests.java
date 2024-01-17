/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import org.opensearch.client.Client;
import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.node.Node;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporterType;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsLocalIndexExporter;
import org.opensearch.plugin.insights.rules.model.SearchQueryLatencyRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

/**
 * Unit Tests for {@link TopQueriesByLatencyService}.
 */
public class TopQueriesByLatencyServiceTests extends OpenSearchTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;
    private ThreadPool threadPool;
    private TopQueriesByLatencyService topQueriesByLatencyService;

    @Before
    public void setup() {
        final Client client = mock(Client.class);
        final Settings settings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "top n queries tests").build();
        deterministicTaskQueue = new DeterministicTaskQueue(settings, random());
        threadPool = deterministicTaskQueue.getThreadPool();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_TYPE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_INTERVAL);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_IDENTIFIER);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, threadPool);
        topQueriesByLatencyService = new TopQueriesByLatencyService(threadPool, clusterService, client);
        topQueriesByLatencyService.setEnableCollect(true);
        topQueriesByLatencyService.setTopNSize(Integer.MAX_VALUE);
        topQueriesByLatencyService.setWindowSize(new TimeValue(Long.MAX_VALUE));
    }

    @After
    public void shutdown() {
        topQueriesByLatencyService.stop();
    }

    public void testIngestQueryDataWithLargeWindow() {
        final List<SearchQueryLatencyRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);
        for (SearchQueryLatencyRecord record : records) {
            topQueriesByLatencyService.ingestQueryData(
                record.getTimestamp(),
                record.getSearchType(),
                record.getSource(),
                record.getTotalShards(),
                record.getIndices(),
                record.getPropertyMap(),
                record.getPhaseLatencyMap(),
                record.getValue()
            );
        }
        runUntilTimeoutOrFinish(deterministicTaskQueue, 5000);
        assertTrue(QueryInsightsTestUtils.checkRecordsEqualsWithoutOrder(topQueriesByLatencyService.getQueryData(), records));
    }

    public void testConcurrentIngestQueryDataWithLargeWindow() {
        final List<SearchQueryLatencyRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);

        int numRequests = records.size();
        for (int i = 0; i < numRequests; i++) {
            int finalI = i;
            threadPool.schedule(() -> {
                topQueriesByLatencyService.ingestQueryData(
                    records.get(finalI).getTimestamp(),
                    records.get(finalI).getSearchType(),
                    records.get(finalI).getSource(),
                    records.get(finalI).getTotalShards(),
                    records.get(finalI).getIndices(),
                    records.get(finalI).getPropertyMap(),
                    records.get(finalI).getPhaseLatencyMap(),
                    records.get(finalI).getValue()
                );
            }, TimeValue.ZERO, ThreadPool.Names.GENERIC);
        }
        runUntilTimeoutOrFinish(deterministicTaskQueue, 5000);
        assertTrue(QueryInsightsTestUtils.checkRecordsEqualsWithoutOrder(topQueriesByLatencyService.getQueryData(), records));
    }

    public void testSmallWindowClearOutdatedData() {
        final List<SearchQueryLatencyRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);
        topQueriesByLatencyService.setWindowSize(new TimeValue(-1));

        for (SearchQueryLatencyRecord record : records) {
            topQueriesByLatencyService.ingestQueryData(
                record.getTimestamp(),
                record.getSearchType(),
                record.getSource(),
                record.getTotalShards(),
                record.getIndices(),
                record.getPropertyMap(),
                record.getPhaseLatencyMap(),
                record.getValue()
            );
        }
        runUntilTimeoutOrFinish(deterministicTaskQueue, 5000);
        assertEquals(0, topQueriesByLatencyService.getQueryData().size());
    }

    public void testSmallNSize() {
        final List<SearchQueryLatencyRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);
        topQueriesByLatencyService.setTopNSize(1);

        for (SearchQueryLatencyRecord record : records) {
            topQueriesByLatencyService.ingestQueryData(
                record.getTimestamp(),
                record.getSearchType(),
                record.getSource(),
                record.getTotalShards(),
                record.getIndices(),
                record.getPropertyMap(),
                record.getPhaseLatencyMap(),
                record.getValue()
            );
        }
        runUntilTimeoutOrFinish(deterministicTaskQueue, 5000);
        assertEquals(1, topQueriesByLatencyService.getQueryData().size());
    }

    public void testIngestQueryDataWithInvalidData() {
        final SearchQueryLatencyRecord record = QueryInsightsTestUtils.generateQueryInsightRecords(1).get(0);
        topQueriesByLatencyService.ingestQueryData(
            -1L,
            record.getSearchType(),
            record.getSource(),
            record.getTotalShards(),
            record.getIndices(),
            record.getPropertyMap(),
            record.getPhaseLatencyMap(),
            record.getValue()
        );
        assertEquals(0, topQueriesByLatencyService.getQueryData().size());

        topQueriesByLatencyService.ingestQueryData(
            record.getTimestamp(),
            record.getSearchType(),
            record.getSource(),
            -1,
            record.getIndices(),
            record.getPropertyMap(),
            record.getPhaseLatencyMap(),
            record.getValue()
        );
        assertEquals(0, topQueriesByLatencyService.getQueryData().size());

    }

    public void testValidateTopNSize() {
        assertThrows(
            IllegalArgumentException.class,
            () -> { topQueriesByLatencyService.validateTopNSize(QueryInsightsSettings.MAX_N_SIZE + 1); }
        );
    }

    public void testValidateWindowSize() {
        assertThrows(IllegalArgumentException.class, () -> {
            topQueriesByLatencyService.validateWindowSize(
                new TimeValue(QueryInsightsSettings.MAX_WINDOW_SIZE.getSeconds() + 1, TimeUnit.SECONDS)
            );
        });
    }

    public void testValidateInterval() {
        assertThrows(IllegalArgumentException.class, () -> {
            topQueriesByLatencyService.validateExportInterval(
                new TimeValue(QueryInsightsSettings.MIN_EXPORT_INTERVAL.getSeconds() - 1, TimeUnit.SECONDS)
            );
        });
    }

    public void testSetExporterTypeWhenDisabled() {
        topQueriesByLatencyService.setExporterEnabled(false);
        topQueriesByLatencyService.setExporterType(QueryInsightsExporterType.LOCAL_INDEX);
        assertNull(topQueriesByLatencyService.exporter);
    }

    public void testSetExporterTypeWhenEnabled() {
        topQueriesByLatencyService.setExporterEnabled(true);
        topQueriesByLatencyService.setExporterType(QueryInsightsExporterType.LOCAL_INDEX);
        assertEquals(QueryInsightsLocalIndexExporter.class, topQueriesByLatencyService.exporter.getClass());
    }

    public void testSetExporterEnabled() {
        topQueriesByLatencyService.setExporterEnabled(true);
        assertEquals(QueryInsightsLocalIndexExporter.class, topQueriesByLatencyService.exporter.getClass());
    }

    public void testSetExporterDisabled() {
        topQueriesByLatencyService.setExporterEnabled(false);
        assertNull(topQueriesByLatencyService.exporter);
    }

    public void testChangeIdentifierWhenEnabled() {
        topQueriesByLatencyService.setExporterEnabled(true);
        topQueriesByLatencyService.setExporterIdentifier("changed");
        assertEquals("changed", topQueriesByLatencyService.exporter.getIdentifier());
    }

    public void testChangeIdentifierWhenDisabled() {
        topQueriesByLatencyService.setExporterEnabled(false);
        topQueriesByLatencyService.setExporterIdentifier("changed");
        assertNull(topQueriesByLatencyService.exporter);
    }

    public void testChangeIntervalWhenEnabled() {
        topQueriesByLatencyService.setExporterEnabled(true);
        TimeValue newInterval = TimeValue.timeValueMillis(randomLongBetween(1, 9999));
        topQueriesByLatencyService.setExportInterval(newInterval);
        assertEquals(newInterval, topQueriesByLatencyService.getExportInterval());
    }

    public void testChangeIntervalWhenDisabled() {
        topQueriesByLatencyService.setExporterEnabled(false);
        TimeValue newInterval = TimeValue.timeValueMillis(randomLongBetween(1, 9999));
        topQueriesByLatencyService.setExportInterval(newInterval);
        assertNull(topQueriesByLatencyService.exporter);
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

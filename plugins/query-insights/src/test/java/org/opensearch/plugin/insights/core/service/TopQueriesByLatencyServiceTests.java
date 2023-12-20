/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.rules.model.SearchQueryLatencyRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;

import static org.mockito.Mockito.mock;

/**
 * Unit Tests for {@link TopQueriesByLatencyService}.
 */
public class TopQueriesByLatencyServiceTests extends OpenSearchTestCase {

    public void testIngestQueryDataWithLargeWindow() {
        final Client client = mock(Client.class);
        final ThreadPool threadPool = mock(ThreadPool.class);

        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE);

        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);

        final List<SearchQueryLatencyRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);

        TopQueriesByLatencyService topQueriesByLatencyService = new TopQueriesByLatencyService(threadPool, clusterService, client);
        topQueriesByLatencyService.setEnabled(true);
        topQueriesByLatencyService.setTopNSize(Integer.MAX_VALUE);
        topQueriesByLatencyService.setWindowSize(new TimeValue(Long.MAX_VALUE));

        for (SearchQueryLatencyRecord record : records) {
            topQueriesByLatencyService.ingestQueryData(
                record.getTimestamp(),
                record.getSearchType(),
                record.getSource(),
                record.getTotalShards(),
                record.getIndices(),
                record.getPropertyMap(),
                record.getPhaseLatencyMap()
            );
        }
        assertTrue(QueryInsightsTestUtils.checkRecordsEqualsWithoutOrder(topQueriesByLatencyService.getQueryData(), records));
    }

    public void testConcurrentIngestQueryDataWithLargeWindow() throws InterruptedException {
        final Client client = mock(Client.class);
        final ThreadPool threadPool = mock(ThreadPool.class);

        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE);

        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);

        final List<SearchQueryLatencyRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);

        TopQueriesByLatencyService topQueriesByLatencyService = new TopQueriesByLatencyService(threadPool, clusterService, client);
        topQueriesByLatencyService.setEnabled(true);
        topQueriesByLatencyService.setTopNSize(Integer.MAX_VALUE);
        topQueriesByLatencyService.setWindowSize(new TimeValue(Long.MAX_VALUE));

        int numRequests = records.size();
        Thread[] threads = new Thread[numRequests];
        Phaser phaser = new Phaser(numRequests + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numRequests);

        for (int i = 0; i < numRequests; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                topQueriesByLatencyService.ingestQueryData(
                    records.get(finalI).getTimestamp(),
                    records.get(finalI).getSearchType(),
                    records.get(finalI).getSource(),
                    records.get(finalI).getTotalShards(),
                    records.get(finalI).getIndices(),
                    records.get(finalI).getPropertyMap(),
                    records.get(finalI).getPhaseLatencyMap()
                );
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();

        assertTrue(QueryInsightsTestUtils.checkRecordsEqualsWithoutOrder(topQueriesByLatencyService.getQueryData(), records));
    }

    public void testSmallWindowClearOutdatedData() {
        final Client client = mock(Client.class);
        final ThreadPool threadPool = mock(ThreadPool.class);

        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE);

        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);

        final List<SearchQueryLatencyRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);

        TopQueriesByLatencyService topQueriesByLatencyService = new TopQueriesByLatencyService(threadPool, clusterService, client);
        topQueriesByLatencyService.setEnabled(true);
        topQueriesByLatencyService.setTopNSize(Integer.MAX_VALUE);
        topQueriesByLatencyService.setWindowSize(new TimeValue(-1));

        for (SearchQueryLatencyRecord record : records) {
            topQueriesByLatencyService.ingestQueryData(
                record.getTimestamp(),
                record.getSearchType(),
                record.getSource(),
                record.getTotalShards(),
                record.getIndices(),
                record.getPropertyMap(),
                record.getPhaseLatencyMap()
            );
        }
        assertEquals(0, topQueriesByLatencyService.getQueryData().size());
    }

    public void testSmallNSize() {
        final Client client = mock(Client.class);
        final ThreadPool threadPool = mock(ThreadPool.class);

        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE);

        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);

        final List<SearchQueryLatencyRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(10);

        TopQueriesByLatencyService topQueriesByLatencyService = new TopQueriesByLatencyService(threadPool, clusterService, client);
        topQueriesByLatencyService.setEnabled(true);
        topQueriesByLatencyService.setTopNSize(1);
        topQueriesByLatencyService.setWindowSize(new TimeValue(Long.MAX_VALUE));

        for (SearchQueryLatencyRecord record : records) {
            topQueriesByLatencyService.ingestQueryData(
                record.getTimestamp(),
                record.getSearchType(),
                record.getSource(),
                record.getTotalShards(),
                record.getIndices(),
                record.getPropertyMap(),
                record.getPhaseLatencyMap()
            );
        }
        assertEquals(1, topQueriesByLatencyService.getQueryData().size());
    }
}

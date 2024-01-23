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
import org.opensearch.node.Node;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsExporterType;
import org.opensearch.plugin.insights.core.exporter.QueryInsightsLocalIndexExporter;
import org.opensearch.plugin.insights.rules.model.SearchQueryLatencyRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit Tests for {@link QueryInsightsService}.
 */
public class QueryInsightsServiceTests extends OpenSearchTestCase {
    private ThreadPool threadPool = mock(ThreadPool.class);
    private DummyQueryInsightsService dummyQueryInsightsService;
    @SuppressWarnings("unchecked")
    private QueryInsightsLocalIndexExporter<SearchQueryLatencyRecord> exporter = mock(QueryInsightsLocalIndexExporter.class);

    static class DummyQueryInsightsService extends QueryInsightsService<
        SearchQueryLatencyRecord,
        ArrayList<SearchQueryLatencyRecord>,
        QueryInsightsLocalIndexExporter<SearchQueryLatencyRecord>> {
        public DummyQueryInsightsService(
            ThreadPool threadPool,
            ClusterService clusterService,
            Client client,
            QueryInsightsLocalIndexExporter<SearchQueryLatencyRecord> exporter
        ) {
            super(threadPool, new ArrayList<>(), exporter);
        }

        @Override
        public void clearOutdatedData() {}

        @Override
        public void resetExporter(boolean enabled, QueryInsightsExporterType type, String identifier) {}

        public void doStart() {
            super.doStart();
        }

        public void doStop() {
            super.doStop();
        }
    }

    @Before
    public void setup() {
        final Client client = mock(Client.class);
        final Settings settings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "top n queries tests").build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_TYPE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_INTERVAL);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_EXPORTER_IDENTIFIER);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, threadPool);
        dummyQueryInsightsService = new DummyQueryInsightsService(threadPool, clusterService, client, exporter);
        when(threadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(new Scheduler.Cancellable() {
            @Override
            public boolean cancel() {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }
        });
    }

    public void testIngestDataWhenFeatureNotEnabled() {
        dummyQueryInsightsService.setEnableCollect(false);
        final List<SearchQueryLatencyRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(1);
        dummyQueryInsightsService.ingestQueryData(records.get(0));
        assertThrows(IllegalArgumentException.class, () -> { dummyQueryInsightsService.getQueryData(); });
    }

    public void testIngestDataWhenFeatureEnabled() {
        dummyQueryInsightsService.setEnableCollect(true);
        final List<SearchQueryLatencyRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(1);
        dummyQueryInsightsService.ingestQueryData(records.get(0));
        assertEquals(records.get(0), dummyQueryInsightsService.getQueryData().get(0));
    }

    public void testClearAllData() {
        dummyQueryInsightsService.setEnableCollect(true);
        dummyQueryInsightsService.setEnableCollect(true);
        final List<SearchQueryLatencyRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(1);
        dummyQueryInsightsService.ingestQueryData(records.get(0));
        dummyQueryInsightsService.clearAllData();
        assertEquals(0, dummyQueryInsightsService.getQueryData().size());
    }

    public void testDoStartAndStop() throws IOException {
        dummyQueryInsightsService.setEnableCollect(true);
        dummyQueryInsightsService.setEnableExport(true);
        dummyQueryInsightsService.doStart();
        verify(threadPool, times(1)).scheduleWithFixedDelay(any(), any(), any());
        dummyQueryInsightsService.doStop();
        verify(exporter, times(1)).export(any());
    }
}

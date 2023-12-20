/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.rules.model.SearchQueryLatencyRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit Tests for {@link QueryInsightsLocalIndexExporter}.
 */
public class QueryInsightsLocalIndexExporterTests extends OpenSearchTestCase {
    private String LOCAL_INDEX_NAME = "top-queries";

    public void testExportWhenIndexExists() throws IOException {
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        Client client = mock(Client.class);
        when(client.admin()).thenReturn(adminClient);

        final List<SearchQueryLatencyRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(5, 10);
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .timeout(TimeValue.timeValueSeconds(60));
        for (SearchQueryLatencyRecord record : records) {
            bulkRequest.add(
                new IndexRequest(LOCAL_INDEX_NAME).source(record.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            );
        }

        ClusterState clusterState = mock(ClusterState.class);
        RoutingTable mockRoutingTable = mock(RoutingTable.class);
        when(mockRoutingTable.hasIndex(anyString())).thenReturn(true);
        when(clusterState.getRoutingTable()).thenReturn(mockRoutingTable);
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE);

        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(clusterState);

        QueryInsightsLocalIndexExporter<SearchQueryLatencyRecord> queryInsightLocalIndexExporter = new QueryInsightsLocalIndexExporter<>(
            true,
            clusterService,
            client,
            LOCAL_INDEX_NAME,
            null
        );

        queryInsightLocalIndexExporter.export(records);

        verify(client, times(1)).bulk(
            argThat((BulkRequest request) -> request.requests().toString().equals(bulkRequest.requests().toString())),
            any()
        );
    }

    public void testConcurrentExportWhenIndexExists() throws IOException, InterruptedException {
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        Client client = mock(Client.class);
        when(client.admin()).thenReturn(adminClient);

        final List<SearchQueryLatencyRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(5, 10);
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .timeout(TimeValue.timeValueSeconds(60));
        for (SearchQueryLatencyRecord record : records) {
            bulkRequest.add(
                new IndexRequest(LOCAL_INDEX_NAME).source(record.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            );
        }

        ClusterState clusterState = mock(ClusterState.class);
        RoutingTable mockRoutingTable = mock(RoutingTable.class);
        when(mockRoutingTable.hasIndex(anyString())).thenReturn(true);
        when(clusterState.getRoutingTable()).thenReturn(mockRoutingTable);
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE);

        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(clusterState);

        int numBulk = 50;
        Thread[] threads = new Thread[numBulk];
        Phaser phaser = new Phaser(numBulk + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numBulk);

        final List<QueryInsightsLocalIndexExporter<SearchQueryLatencyRecord>> queryInsightLocalIndexExporters = new ArrayList<>();
        for (int i = 0; i < numBulk; i++) {
            queryInsightLocalIndexExporters.add(
                new QueryInsightsLocalIndexExporter<>(true, clusterService, client, LOCAL_INDEX_NAME, null)
            );
        }

        for (int i = 0; i < numBulk; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                QueryInsightsLocalIndexExporter<SearchQueryLatencyRecord> thisExporter = queryInsightLocalIndexExporters.get(finalI);
                try {
                    thisExporter.export(records);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();

        verify(client, times(numBulk)).bulk(
            argThat((BulkRequest request) -> request.requests().toString().equals(bulkRequest.requests().toString())),
            any()
        );
    }

    public void testExportWhenIndexNotExists() throws IOException {
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        Client client = mock(Client.class);
        when(client.admin()).thenReturn(adminClient);

        final List<SearchQueryLatencyRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(5, 10);
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .timeout(TimeValue.timeValueSeconds(60));
        for (SearchQueryLatencyRecord record : records) {
            bulkRequest.add(
                new IndexRequest(LOCAL_INDEX_NAME).source(record.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            );
        }

        ClusterState clusterState = mock(ClusterState.class);
        RoutingTable mockRoutingTable = mock(RoutingTable.class);
        when(mockRoutingTable.hasIndex(anyString())).thenReturn(false);
        when(clusterState.getRoutingTable()).thenReturn(mockRoutingTable);
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE);

        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(clusterState);

        final int length = randomIntBetween(1, 1024);
        ByteArrayInputStream is = new ByteArrayInputStream(new byte[length]);
        InputStreamStreamInput streamInput = new InputStreamStreamInput(is);

        QueryInsightsLocalIndexExporter<SearchQueryLatencyRecord> queryInsightLocalIndexExporter = new QueryInsightsLocalIndexExporter<>(
            true,
            clusterService,
            client,
            LOCAL_INDEX_NAME,
            streamInput
        );

        queryInsightLocalIndexExporter.export(records);

        verify(indicesAdminClient, times(1)).create(
            argThat((CreateIndexRequest request) -> request.index().equals(LOCAL_INDEX_NAME)),
            any()
        );
    }

    public void testConcurrentExportWhenIndexNotExists() throws IOException, InterruptedException {
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        Client client = mock(Client.class);
        when(client.admin()).thenReturn(adminClient);

        final List<SearchQueryLatencyRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(5, 10);
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .timeout(TimeValue.timeValueSeconds(60));
        for (SearchQueryLatencyRecord record : records) {
            bulkRequest.add(
                new IndexRequest(LOCAL_INDEX_NAME).source(record.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            );
        }

        ClusterState clusterState = mock(ClusterState.class);
        RoutingTable mockRoutingTable = mock(RoutingTable.class);
        when(mockRoutingTable.hasIndex(anyString())).thenReturn(false).thenReturn(true);
        when(clusterState.getRoutingTable()).thenReturn(mockRoutingTable);
        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE);

        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(clusterState);

        int numBulk = 50;
        Thread[] threads = new Thread[numBulk];
        Phaser phaser = new Phaser(numBulk + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numBulk);

        final int length = randomIntBetween(1, 1024);
        ByteArrayInputStream is = new ByteArrayInputStream(new byte[length]);
        InputStreamStreamInput streamInput = new InputStreamStreamInput(is);

        final List<QueryInsightsLocalIndexExporter<SearchQueryLatencyRecord>> queryInsightLocalIndexExporters = new ArrayList<>();
        for (int i = 0; i < numBulk; i++) {
            queryInsightLocalIndexExporters.add(
                new QueryInsightsLocalIndexExporter<>(true, clusterService, client, LOCAL_INDEX_NAME, streamInput)
            );
        }

        for (int i = 0; i < numBulk; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                QueryInsightsLocalIndexExporter<SearchQueryLatencyRecord> thisExporter = queryInsightLocalIndexExporters.get(finalI);
                try {
                    thisExporter.export(records);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();

        verify(indicesAdminClient, times(1)).create(
            argThat((CreateIndexRequest request) -> request.index().equals(LOCAL_INDEX_NAME)),
            any()
        );
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.listener;

import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.action.search.SearchType;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.insights.core.service.TopQueriesByLatencyService;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Phaser;

import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit Tests for {@link SearchQueryLatencyListener}.
 */
public class SearchQueryLatencyListenerTests extends OpenSearchTestCase {

    public void testOnRequestEnd() {
        final SearchRequestContext searchRequestContext = mock(SearchRequestContext.class);
        final SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        final SearchRequest searchRequest = mock(SearchRequest.class);
        final TopQueriesByLatencyService topQueriesByLatencyService = mock(TopQueriesByLatencyService.class);

        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE);

        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);

        Long timestamp = System.currentTimeMillis() - 100L;
        SearchType searchType = SearchType.QUERY_THEN_FETCH;

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(new TermsAggregationBuilder("agg1").userValueTypeHint(ValueType.STRING).field("type.keyword"));
        searchSourceBuilder.size(0);

        String[] indices = new String[] { "index-1", "index-2" };

        Map<String, Long> phaseLatencyMap = new HashMap<>();
        phaseLatencyMap.put("expand", 0L);
        phaseLatencyMap.put("query", 20L);
        phaseLatencyMap.put("fetch", 1L);

        int numberOfShards = 10;

        SearchQueryLatencyListener searchQueryLatencyListener = new SearchQueryLatencyListener(clusterService, topQueriesByLatencyService);

        when(searchRequest.getOrCreateAbsoluteStartMillis()).thenReturn(timestamp);
        when(searchRequest.searchType()).thenReturn(searchType);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        when(searchRequest.indices()).thenReturn(indices);
        when(searchRequestContext.phaseTookMap()).thenReturn(phaseLatencyMap);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchPhaseContext.getNumShards()).thenReturn(numberOfShards);

        searchQueryLatencyListener.onRequestEnd(searchPhaseContext, searchRequestContext);

        verify(topQueriesByLatencyService, times(1)).ingestQueryData(
            eq(timestamp),
            eq(searchType),
            eq(searchSourceBuilder.toString()),
            eq(numberOfShards),
            eq(indices),
            anyMap(),
            eq(phaseLatencyMap)
        );
    }

    public void testConcurrentOnRequestEnd() throws InterruptedException {
        final SearchRequestContext searchRequestContext = mock(SearchRequestContext.class);
        final SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        final SearchRequest searchRequest = mock(SearchRequest.class);
        final TopQueriesByLatencyService topQueriesByLatencyService = mock(TopQueriesByLatencyService.class);

        Settings.Builder settingsBuilder = Settings.builder();
        Settings settings = settingsBuilder.build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE);
        clusterSettings.registerSetting(QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE);

        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);

        Long timestamp = System.currentTimeMillis() - 100L;
        SearchType searchType = SearchType.QUERY_THEN_FETCH;

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(new TermsAggregationBuilder("agg1").userValueTypeHint(ValueType.STRING).field("type.keyword"));
        searchSourceBuilder.size(0);

        String[] indices = new String[] { "index-1", "index-2" };

        Map<String, Long> phaseLatencyMap = new HashMap<>();
        phaseLatencyMap.put("expand", 0L);
        phaseLatencyMap.put("query", 20L);
        phaseLatencyMap.put("fetch", 1L);

        int numberOfShards = 10;

        final List<SearchQueryLatencyListener> searchListenersList = new ArrayList<>();

        when(searchRequest.getOrCreateAbsoluteStartMillis()).thenReturn(timestamp);
        when(searchRequest.searchType()).thenReturn(searchType);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        when(searchRequest.indices()).thenReturn(indices);
        when(searchRequestContext.phaseTookMap()).thenReturn(phaseLatencyMap);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchPhaseContext.getNumShards()).thenReturn(numberOfShards);

        int numRequests = 50;
        Thread[] threads = new Thread[numRequests];
        Phaser phaser = new Phaser(numRequests + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numRequests);

        for (int i = 0; i < numRequests; i++) {
            searchListenersList.add(new SearchQueryLatencyListener(clusterService, topQueriesByLatencyService));
        }

        for (int i = 0; i < numRequests; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                SearchQueryLatencyListener thisListener = searchListenersList.get(finalI);
                thisListener.onRequestEnd(searchPhaseContext, searchRequestContext);
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();

        verify(topQueriesByLatencyService, times(numRequests)).ingestQueryData(
            eq(timestamp),
            eq(searchType),
            eq(searchSourceBuilder.toString()),
            eq(numberOfShards),
            eq(indices),
            anyMap(),
            eq(phaseLatencyMap)
        );
    }
}

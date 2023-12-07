/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SearchQueryLatencyListenerTests extends OpenSearchTestCase {

    public void testOnRequestEnd() {
        final SearchRequestContext searchRequestContext = mock(SearchRequestContext.class);
        final SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        final SearchRequest searchRequest = mock(SearchRequest.class);
        final QueryLatencyInsightService queryLatencyInsightService = mock(QueryLatencyInsightService.class);

        Long timestamp = System.currentTimeMillis() - 100L;
        SearchType searchType = SearchType.QUERY_THEN_FETCH;

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(
            new TermsAggregationBuilder("agg1")
                .userValueTypeHint(ValueType.STRING)
                .field("type.keyword")
        );
        searchSourceBuilder.size(0);

        String[] indices = new String[]{"index-1", "index-2"};

        Map<String, Long> phaseLatencyMap = new HashMap<>();
        phaseLatencyMap.put("expand", 0L);
        phaseLatencyMap.put("query", 20L);
        phaseLatencyMap.put("fetch", 1L);

        int numberOfShards = 10;


        SearchQueryLatencyListener searchQueryLatencyListener = new SearchQueryLatencyListener(queryLatencyInsightService);
        final List<SearchRequestOperationsListener> searchListenersList = new ArrayList<>(
            List.of(searchQueryLatencyListener)
        );

        when(searchRequestContext.getSearchRequestOperationsListener()).thenReturn(
            new SearchRequestOperationsListener.CompositeListener(searchListenersList, logger)
        );
        when(searchRequest.getOrCreateAbsoluteStartMillis()).thenReturn(timestamp);
        when(searchRequest.searchType()).thenReturn(searchType);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        when(searchRequest.indices()).thenReturn(indices);
        when(searchRequestContext.phaseTookMap()).thenReturn(phaseLatencyMap);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchPhaseContext.getNumShards()).thenReturn(numberOfShards);


        searchRequestContext.getSearchRequestOperationsListener().onRequestEnd(searchPhaseContext, searchRequestContext);

        verify(queryLatencyInsightService, times(1)).ingestQueryData(
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
        final SearchPhaseContext searchPhaseContext = mock(SearchPhaseContext.class);
        final SearchRequest searchRequest = mock(SearchRequest.class);
        final QueryLatencyInsightService queryLatencyInsightService = mock(QueryLatencyInsightService.class);

        Long timestamp = System.currentTimeMillis() - 100L;
        SearchType searchType = SearchType.QUERY_THEN_FETCH;

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(
            new TermsAggregationBuilder("agg1")
                .userValueTypeHint(ValueType.STRING)
                .field("type.keyword")
        );
        searchSourceBuilder.size(0);

        String[] indices = new String[]{"index-1", "index-2"};

        Map<String, Long> phaseLatencyMap = new HashMap<>();
        phaseLatencyMap.put("expand", 0L);
        phaseLatencyMap.put("query", 20L);
        phaseLatencyMap.put("fetch", 1L);

        int numberOfShards = 10;


        SearchQueryLatencyListener searchQueryLatencyListener = new SearchQueryLatencyListener(queryLatencyInsightService);
        final List<SearchRequestOperationsListener> searchListenersList = new ArrayList<>(
            List.of(searchQueryLatencyListener)
        );

        when(searchRequest.getOrCreateAbsoluteStartMillis()).thenReturn(timestamp);
        when(searchRequest.searchType()).thenReturn(searchType);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);
        when(searchRequest.indices()).thenReturn(indices);
        when(searchPhaseContext.getRequest()).thenReturn(searchRequest);
        when(searchPhaseContext.getNumShards()).thenReturn(numberOfShards);


        int numRequests = 50;
        Thread[] threads = new Thread[numRequests];
        Phaser phaser = new Phaser(numRequests + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numRequests);

        ArrayList<SearchRequestContext> searchRequestContexts = new ArrayList<>();
        for (int i = 0; i < numRequests; i++) {
            SearchRequestContext searchRequestContext = new SearchRequestContext(
                new SearchRequestOperationsListener.CompositeListener(searchListenersList, logger)
            );
            phaseLatencyMap.forEach(searchRequestContext::updatePhaseTookMap);
            searchRequestContexts.add(searchRequestContext);
        }

        for (int i = 0; i < numRequests; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                SearchRequestContext thisContext = searchRequestContexts.get(finalI);
                thisContext
                    .getSearchRequestOperationsListener()
                    .onRequestEnd(searchPhaseContext, thisContext);
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();

        verify(queryLatencyInsightService, times(numRequests)).ingestQueryData(
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

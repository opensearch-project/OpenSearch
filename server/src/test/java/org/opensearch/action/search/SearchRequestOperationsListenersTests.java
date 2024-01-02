/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class SearchRequestOperationsListenersTests extends OpenSearchTestCase {
    public void testAddAndGetListeners() {
        SearchRequestOperationsListener testListener = createTestSearchRequestOperationsListener();
        SearchRequestOperationsListeners requestListeners = new SearchRequestOperationsListeners(testListener);
        assertEquals(1, requestListeners.getListeners().size());
        assertEquals(testListener, requestListeners.getListeners().get(0));
    }

    public void testStandardListenersEnabled() {
        SearchRequestOperationsListener testListener1 = createTestSearchRequestOperationsListener();
        SearchRequestOperationsListener testListener2 = createTestSearchRequestOperationsListener();
        SearchRequestOperationsListeners requestListeners = new SearchRequestOperationsListeners(testListener1, testListener2);
        testListener2.setEnabled(true);
        SearchRequestOperationsListener.CompositeListener compositeListener = requestListeners.buildCompositeListener(logger);
        List<SearchRequestOperationsListener> listeners = compositeListener.getListeners();
        assertEquals(1, listeners.size());
        assertEquals(testListener2, listeners.get(0));
        assertEquals(2, requestListeners.getListeners().size());
        assertEquals(testListener1, requestListeners.getListeners().get(0));
        assertEquals(testListener2, requestListeners.getListeners().get(1));
    }

    public void testStandardListenersAndTimeProvider() {
        SearchRequestOperationsListener testListener1 = createTestSearchRequestOperationsListener();
        SearchRequestOperationsListeners requestListeners = new SearchRequestOperationsListeners(testListener1);

        testListener1.setEnabled(true);
        TransportSearchAction.SearchTimeProvider timeProviderListener = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );
        SearchSourceBuilder source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest().source(source);
        searchRequest.setPhaseTook(true);
        timeProviderListener.setEnabled(false, searchRequest);
        SearchRequestOperationsListener.CompositeListener compositeListener = requestListeners.buildCompositeListener(
            logger,
            timeProviderListener
        );
        List<SearchRequestOperationsListener> listeners = compositeListener.getListeners();
        assertEquals(2, listeners.size());
        assertEquals(testListener1, listeners.get(0));
        assertEquals(timeProviderListener, listeners.get(1));
        assertEquals(1, requestListeners.getListeners().size());
        assertEquals(testListener1, requestListeners.getListeners().get(0));
    }

    public void testStandardListenersDisabledAndTimeProvider() {
        SearchRequestOperationsListener testListener1 = createTestSearchRequestOperationsListener();
        SearchRequestOperationsListeners requestListeners = new SearchRequestOperationsListeners(testListener1);
        TransportSearchAction.SearchTimeProvider timeProviderListener = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );
        SearchSourceBuilder source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest().source(source);
        searchRequest.setPhaseTook(true);
        timeProviderListener.setEnabled(false, searchRequest);
        SearchRequestOperationsListener.CompositeListener compositeListener = requestListeners.buildCompositeListener(
            logger,
            timeProviderListener
        );
        List<SearchRequestOperationsListener> listeners = compositeListener.getListeners();
        assertEquals(1, listeners.size());
        assertEquals(timeProviderListener, listeners.get(0));
        assertEquals(1, requestListeners.getListeners().size());
        assertEquals(testListener1, requestListeners.getListeners().get(0));
        assertFalse(requestListeners.getListeners().get(0).getEnabled());
    }

    public void testStandardListenerAndTimeProviderDisabled() {
        SearchRequestOperationsListener testListener1 = createTestSearchRequestOperationsListener();
        SearchRequestOperationsListeners requestListeners = new SearchRequestOperationsListeners(testListener1);

        testListener1.setEnabled(true);
        SearchRequestOperationsListener timeProviderListener = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );
        SearchSourceBuilder source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest().source(source);
        searchRequest.setPhaseTook(false);
        SearchRequestOperationsListener.CompositeListener compositeListener = requestListeners.buildCompositeListener(
            logger,
            timeProviderListener
        );
        List<SearchRequestOperationsListener> listeners = compositeListener.getListeners();
        assertEquals(1, listeners.size());
        assertEquals(testListener1, listeners.get(0));
        assertEquals(1, requestListeners.getListeners().size());
        assertEquals(testListener1, requestListeners.getListeners().get(0));
    }

    public SearchRequestOperationsListener createTestSearchRequestOperationsListener() {
        return new SearchRequestOperationsListener() {
            @Override
            void onPhaseStart(SearchPhaseContext context) {}

            @Override
            void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {}

            @Override
            void onPhaseFailure(SearchPhaseContext context) {}
        };
    }
}

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

public class SearchRequestOperationsCompositeListenerFactoryTests extends OpenSearchTestCase {
    public void testAddAndGetListeners() {
        SearchRequestOperationsListener testListener = createTestSearchRequestOperationsListener();
        SearchRequestOperationsCompositeListenerFactory requestListeners = new SearchRequestOperationsCompositeListenerFactory(
            testListener
        );
        assertEquals(1, requestListeners.getListeners().size());
        assertEquals(testListener, requestListeners.getListeners().get(0));
    }

    public void testStandardListenersEnabled() {
        SearchRequestOperationsListener testListener1 = createTestSearchRequestOperationsListener();
        SearchRequestOperationsListener testListener2 = createTestSearchRequestOperationsListener();
        testListener1.setEnabled(false);
        SearchRequestOperationsCompositeListenerFactory requestListeners = new SearchRequestOperationsCompositeListenerFactory(
            testListener1,
            testListener2
        );
        SearchSourceBuilder source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest().source(source);
        SearchRequestOperationsListener.CompositeListener compositeListener = requestListeners.buildCompositeListener(
            searchRequest,
            logger
        );
        List<SearchRequestOperationsListener> listeners = compositeListener.getListeners();
        assertEquals(1, listeners.size());
        assertEquals(testListener2, listeners.get(0));
        assertEquals(2, requestListeners.getListeners().size());
        assertEquals(testListener1, requestListeners.getListeners().get(0));
        assertEquals(testListener2, requestListeners.getListeners().get(1));
    }

    public void testStandardListenersAndPerRequestListener() {
        SearchRequestOperationsListener testListener1 = createTestSearchRequestOperationsListener();
        SearchRequestOperationsCompositeListenerFactory requestListeners = new SearchRequestOperationsCompositeListenerFactory(
            testListener1
        );
        SearchRequestOperationsListener testListener2 = createTestSearchRequestOperationsListener();
        testListener1.setEnabled(true);
        testListener2.setEnabled(true);
        SearchSourceBuilder source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest().source(source);
        searchRequest.setPhaseTook(true);
        SearchRequestOperationsListener.CompositeListener compositeListener = requestListeners.buildCompositeListener(
            searchRequest,
            logger,
            testListener2
        );
        List<SearchRequestOperationsListener> listeners = compositeListener.getListeners();
        assertEquals(2, listeners.size());
        assertEquals(testListener1, listeners.get(0));
        assertEquals(testListener2, listeners.get(1));
        assertEquals(1, requestListeners.getListeners().size());
        assertEquals(testListener1, requestListeners.getListeners().get(0));
    }

    public void testStandardListenersDisabledAndPerRequestListener() {
        SearchRequestOperationsListener testListener1 = createTestSearchRequestOperationsListener();
        testListener1.setEnabled(false);
        SearchRequestOperationsCompositeListenerFactory requestListeners = new SearchRequestOperationsCompositeListenerFactory(
            testListener1
        );
        SearchRequestOperationsListener testListener2 = createTestSearchRequestOperationsListener();
        SearchSourceBuilder source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest().source(source);
        SearchRequestOperationsListener.CompositeListener compositeListener = requestListeners.buildCompositeListener(
            searchRequest,
            logger,
            testListener2
        );
        List<SearchRequestOperationsListener> listeners = compositeListener.getListeners();
        assertEquals(1, listeners.size());
        assertEquals(testListener2, listeners.get(0));
        assertEquals(1, requestListeners.getListeners().size());
        assertEquals(testListener1, requestListeners.getListeners().get(0));
        assertFalse(requestListeners.getListeners().get(0).isEnabled());
    }

    public void testStandardListenerAndPerRequestListenerDisabled() {
        SearchRequestOperationsListener testListener1 = createTestSearchRequestOperationsListener();
        SearchRequestOperationsCompositeListenerFactory requestListeners = new SearchRequestOperationsCompositeListenerFactory(
            testListener1
        );
        testListener1.setEnabled(true);
        SearchRequestOperationsListener testListener2 = createTestSearchRequestOperationsListener();
        testListener2.setEnabled(false);

        SearchSourceBuilder source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest().source(source);
        searchRequest.setPhaseTook(false);
        SearchRequestOperationsListener.CompositeListener compositeListener = requestListeners.buildCompositeListener(
            searchRequest,
            logger,
            testListener2
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
            protected void onPhaseStart(SearchPhaseContext context) {}

            @Override
            protected void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {}

            @Override
            protected void onPhaseFailure(SearchPhaseContext context, Throwable cause) {}
        };
    }
}

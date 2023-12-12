/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.Field;
import java.util.List;


public class SearchRequestListenerManagerTests extends OpenSearchTestCase {
    public void testAddAndGetListeners() {
        ClusterService clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null
        );
        SearchRequestListenerManager listenerManager = new SearchRequestListenerManager(clusterService);
        SearchRequestOperationsListener testListener = createTestSearchRequestOperationsListener();
        listenerManager.addListener(testListener);
        assertEquals(1, listenerManager.getListeners().size());
        assertEquals(testListener, listenerManager.getListeners().get(0));
    }

    public void testRemoveListeners() {
        ClusterService clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null
        );
        SearchRequestListenerManager listenerManager = new SearchRequestListenerManager(clusterService);
        SearchRequestOperationsListener testListener1 = createTestSearchRequestOperationsListener();
        SearchRequestOperationsListener testListener2 = createTestSearchRequestOperationsListener();
        listenerManager.addListener(testListener1);
        listenerManager.addListener(testListener2);
        assertEquals(2, listenerManager.getListeners().size());
        listenerManager.removeListener(testListener2);
        assertEquals(1, listenerManager.getListeners().size());
        assertEquals(testListener1, listenerManager.getListeners().get(0));
    }

    public void testBuildCompositeListenersWithTimeProvider() throws NoSuchFieldException, IllegalAccessException {
        ClusterService clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null
        );
        SearchRequestListenerManager listenerManager = new SearchRequestListenerManager(clusterService);
        SearchRequestOperationsListener testListener1 = createTestSearchRequestOperationsListener();
        SearchRequestOperationsListener timeProviderListener = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );
        listenerManager.addListener(testListener1);
        SearchSourceBuilder source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest().source(source);
        searchRequest.setPhaseTook(true);
        SearchRequestOperationsListener.CompositeListener compositeListener = listenerManager.buildCompositeListener(
            searchRequest,
            logger,
            timeProviderListener
        );
        Field listenersField = SearchRequestOperationsListener.CompositeListener.class.getDeclaredField("listeners");
        listenersField.setAccessible(true);
        List<SearchRequestOperationsListener> listeners = (List<SearchRequestOperationsListener>) listenersField.get(compositeListener);
        assertEquals(2, listeners.size());
        assertEquals(testListener1, listeners.get(0));
        assertEquals(timeProviderListener, listeners.get(1));
        assertEquals(1, listenerManager.getListeners().size());
        assertEquals(testListener1, listenerManager.getListeners().get(0));
    }

    public void testBuildCompositeListenersWithPhaseTookDisabled() throws NoSuchFieldException, IllegalAccessException {
        ClusterService clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null
        );
        SearchRequestListenerManager listenerManager = new SearchRequestListenerManager(clusterService);
        SearchRequestOperationsListener testListener1 = createTestSearchRequestOperationsListener();
        SearchRequestOperationsListener timeProviderListener = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );
        listenerManager.addListener(testListener1);
        SearchSourceBuilder source = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest().source(source);
        searchRequest.setPhaseTook(false);
        SearchRequestOperationsListener.CompositeListener compositeListener = listenerManager.buildCompositeListener(
            searchRequest,
            logger,
            timeProviderListener
        );
        Field listenersField = SearchRequestOperationsListener.CompositeListener.class.getDeclaredField("listeners");
        listenersField.setAccessible(true);
        List<SearchRequestOperationsListener> listeners = (List<SearchRequestOperationsListener>) listenersField.get(compositeListener);
        assertEquals(1, listeners.size());
        assertEquals(testListener1, listeners.get(0));
        assertEquals(1, listenerManager.getListeners().size());
        assertEquals(testListener1, listenerManager.getListeners().get(0));
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

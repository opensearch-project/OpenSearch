/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchRequestOperationsListenerTests extends OpenSearchTestCase {

    public void testListenersAreExecuted() {
        Map<SearchPhaseName, AtomicInteger> searchPhaseStartMap = new HashMap<>();
        Map<SearchPhaseName, AtomicInteger> searchPhaseEndMap = new HashMap<>();
        Map<SearchPhaseName, AtomicInteger> searchPhaseFailureMap = new HashMap<>();

        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            searchPhaseStartMap.put(searchPhaseName, new AtomicInteger());
            searchPhaseEndMap.put(searchPhaseName, new AtomicInteger());
            searchPhaseFailureMap.put(searchPhaseName, new AtomicInteger());
        }
        SearchRequestOperationsListener testListener = new SearchRequestOperationsListener() {

            @Override
            public void onPhaseStart(SearchPhaseContext context) {
                searchPhaseStartMap.get(context.getCurrentPhase().getSearchPhaseName()).incrementAndGet();
            }

            @Override
            public void onPhaseEnd(SearchPhaseContext context) {
                searchPhaseEndMap.get(context.getCurrentPhase().getSearchPhaseName()).incrementAndGet();
            }

            @Override
            public void onPhaseFailure(SearchPhaseContext context) {
                searchPhaseFailureMap.get(context.getCurrentPhase().getSearchPhaseName()).incrementAndGet();
            }
        };

        int totalListeners = randomIntBetween(1, 10);
        final List<SearchRequestOperationsListener> requestOperationListeners = new ArrayList<>();
        for (int i = 0; i < totalListeners; i++) {
            requestOperationListeners.add(testListener);
        }

        SearchRequestOperationsListener compositeListener = new SearchRequestOperationsListener.CompositeListener(
            requestOperationListeners,
            logger
        );

        SearchPhaseContext ctx = mock(SearchPhaseContext.class);
        SearchPhase searchPhase = mock(SearchPhase.class);

        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            when(ctx.getCurrentPhase()).thenReturn(searchPhase);
            when(searchPhase.getName()).thenReturn(searchPhaseName.getName());
            compositeListener.onPhaseStart(ctx);
            assertEquals(totalListeners, searchPhaseStartMap.get(searchPhaseName).get());
        }
    }
}

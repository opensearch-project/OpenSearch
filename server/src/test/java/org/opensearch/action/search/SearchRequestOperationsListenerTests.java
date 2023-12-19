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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchRequestOperationsListenerTests extends OpenSearchTestCase {

    public void testListenersAreExecuted() {
        Map<SearchPhaseName, SearchRequestStats.StatsHolder> searchPhaseMap = new HashMap<>();

        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            searchPhaseMap.put(searchPhaseName, new SearchRequestStats.StatsHolder());
        }
        SearchRequestOperationsListener testListener = new SearchRequestOperationsListener() {

            @Override
            public void onPhaseStart(SearchPhaseContext context) {
                searchPhaseMap.get(context.getCurrentPhase().getSearchPhaseName()).current.inc();
            }

            @Override
            public void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
                searchPhaseMap.get(context.getCurrentPhase().getSearchPhaseName()).current.dec();
                searchPhaseMap.get(context.getCurrentPhase().getSearchPhaseName()).total.inc();
            }

            @Override
            public void onPhaseFailure(SearchPhaseContext context) {
                searchPhaseMap.get(context.getCurrentPhase().getSearchPhaseName()).current.dec();
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
            when(searchPhase.getSearchPhaseName()).thenReturn(searchPhaseName);
            compositeListener.onPhaseStart(ctx);
            assertEquals(totalListeners, searchPhaseMap.get(searchPhaseName).current.count());
        }
    }
}

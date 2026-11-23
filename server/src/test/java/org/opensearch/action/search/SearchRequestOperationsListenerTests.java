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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

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
                searchPhaseMap.get(context.getCurrentPhase().getSearchPhaseNameOptional().get()).current.inc();
            }

            @Override
            public void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
                searchPhaseMap.get(context.getCurrentPhase().getSearchPhaseNameOptional().get()).current.dec();
                searchPhaseMap.get(context.getCurrentPhase().getSearchPhaseNameOptional().get()).total.inc();
            }

            @Override
            public void onPhaseFailure(SearchPhaseContext context, Throwable cause) {
                searchPhaseMap.get(context.getCurrentPhase().getSearchPhaseNameOptional().get()).current.dec();
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
            when(searchPhase.getSearchPhaseNameOptional()).thenReturn(Optional.of(searchPhaseName));
            compositeListener.onPhaseStart(ctx);
            assertEquals(totalListeners, searchPhaseMap.get(searchPhaseName).current.count());
        }
    }

    public void testRequestEndReachesEveryListener() {
        AtomicInteger ends = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        SearchRequestOperationsListener countingListener = new SearchRequestOperationsListener() {
            @Override
            protected void onRequestEnd(SearchRequestContext searchRequestContext) {
                ends.incrementAndGet();
            }

            @Override
            protected void onRequestFailure(SearchRequestContext searchRequestContext) {
                failures.incrementAndGet();
            }
        };
        SearchRequestOperationsListener failingListener = new SearchRequestOperationsListener() {
            @Override
            protected void onRequestEnd(SearchRequestContext searchRequestContext) {
                throw new IllegalStateException("end");
            }

            @Override
            protected void onRequestFailure(SearchRequestContext searchRequestContext) {
                throw new IllegalStateException("failure");
            }
        };
        List<SearchPhaseContext> legacyEnds = new ArrayList<>();
        List<SearchPhaseContext> legacyFailures = new ArrayList<>();
        SearchRequestOperationsListener legacyListener = new SearchRequestOperationsListener() {
            @Override
            protected void onRequestEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
                legacyEnds.add(context);
            }

            @Override
            protected void onRequestFailure(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
                legacyFailures.add(context);
            }
        };
        SearchRequestOperationsListener compositeListener = new SearchRequestOperationsListener.CompositeListener(
            List.of(failingListener, countingListener, legacyListener),
            logger
        );
        SearchRequestContext searchRequestContext = new SearchRequestContext(compositeListener, new SearchRequest(), () -> null);

        // Without a search phase on this node, only listeners of the single-argument methods hear about it.
        compositeListener.onRequestEnd(searchRequestContext);
        compositeListener.onRequestFailure(searchRequestContext);
        assertEquals(1, ends.get());
        assertEquals(1, failures.get());
        assertTrue(legacyEnds.isEmpty());
        assertTrue(legacyFailures.isEmpty());

        // With one, listeners of the two-argument methods hear about it too, with its context.
        SearchPhaseContext phaseContext = mock(SearchPhaseContext.class);
        searchRequestContext.setPhaseContext(phaseContext);
        compositeListener.onRequestEnd(searchRequestContext);
        compositeListener.onRequestFailure(searchRequestContext);
        // A listener that throws must not stop the listeners after it.
        assertEquals(2, ends.get());
        assertEquals(2, failures.get());
        assertEquals(List.of(phaseContext), legacyEnds);
        assertEquals(List.of(phaseContext), legacyFailures);
    }
}

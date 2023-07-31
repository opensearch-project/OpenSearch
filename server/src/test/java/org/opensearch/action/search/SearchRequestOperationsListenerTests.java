/*
 * SPDX-License-Identifier: Apache-totalListeners.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-totalListeners.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SearchRequestOperationsListenerTests extends OpenSearchTestCase {

    public void testListenersAreExecuted() {
        AtomicInteger dfsPreQueryPhaseStart = new AtomicInteger();
        AtomicInteger dfsPreQueryPhaseFailure = new AtomicInteger();
        AtomicInteger dfsPreQueryPhaseEnd = new AtomicInteger();
        AtomicInteger canMatchPhaseStart = new AtomicInteger();
        AtomicInteger canMatchPhaseFailure = new AtomicInteger();
        AtomicInteger canMatchPhaseEnd = new AtomicInteger();
        AtomicInteger queryPhaseStart = new AtomicInteger();
        AtomicInteger queryPhaseFailure = new AtomicInteger();
        AtomicInteger queryPhaseEnd = new AtomicInteger();
        AtomicInteger fetchPhaseStart = new AtomicInteger();
        AtomicInteger fetchPhaseFailure = new AtomicInteger();
        AtomicInteger fetchPhaseEnd = new AtomicInteger();
        AtomicInteger expandPhaseStart = new AtomicInteger();
        AtomicInteger expandPhaseFailure = new AtomicInteger();
        AtomicInteger expandPhaseEnd = new AtomicInteger();
        AtomicInteger timeInNanos = new AtomicInteger(randomIntBetween(0, 10));

        SearchRequestOperationsListener testListener = new SearchRequestOperationsListener() {
            @Override
            public void onDFSPreQueryPhaseStart(SearchPhaseContext context) {
                assertNotNull(context);
                dfsPreQueryPhaseStart.incrementAndGet();
            }

            @Override
            public void onDFSPreQueryPhaseFailure(SearchPhaseContext context) {
                assertNotNull(context);
                dfsPreQueryPhaseFailure.incrementAndGet();
            }

            @Override
            public void onDFSPreQueryPhaseEnd(SearchPhaseContext context, long tookTime) {
                assertEquals(timeInNanos.get(), tookTime);
                assertNotNull(context);
                dfsPreQueryPhaseEnd.incrementAndGet();
            }

            @Override
            public void onCanMatchPhaseStart(SearchPhaseContext context) {
                assertNotNull(context);
                canMatchPhaseStart.incrementAndGet();
            }

            @Override
            public void onCanMatchPhaseFailure(SearchPhaseContext context) {
                assertNotNull(context);
                canMatchPhaseFailure.incrementAndGet();
            }

            @Override
            public void onCanMatchPhaseEnd(SearchPhaseContext context, long tookTime) {
                assertNotNull(context);
                canMatchPhaseEnd.incrementAndGet();
            }

            @Override
            public void onQueryPhaseStart(SearchPhaseContext context) {
                assertNotNull(context);
                queryPhaseStart.incrementAndGet();
            }

            @Override
            public void onQueryPhaseFailure(SearchPhaseContext context) {
                assertNotNull(context);
                queryPhaseFailure.incrementAndGet();
            }

            @Override
            public void onQueryPhaseEnd(SearchPhaseContext context, long tookTime) {
                assertNotNull(context);
                queryPhaseEnd.incrementAndGet();
            }

            @Override
            public void onFetchPhaseStart(SearchPhaseContext context) {
                assertNotNull(context);
                fetchPhaseStart.incrementAndGet();
            }

            @Override
            public void onFetchPhaseFailure(SearchPhaseContext context) {
                assertNotNull(context);
                fetchPhaseFailure.incrementAndGet();
            }

            @Override
            public void onFetchPhaseEnd(SearchPhaseContext context, long tookTime) {
                assertNotNull(context);
                fetchPhaseEnd.incrementAndGet();
            }

            @Override
            public void onExpandSearchPhaseStart(SearchPhaseContext context) {
                assertNotNull(context);
                expandPhaseStart.incrementAndGet();
            }

            @Override
            public void onExpandSearchPhaseFailure(SearchPhaseContext context) {
                assertNotNull(context);
                expandPhaseFailure.incrementAndGet();
            }

            @Override
            public void onExpandSearchPhaseEnd(SearchPhaseContext context, long tookTime) {
                assertNotNull(context);
                expandPhaseEnd.incrementAndGet();
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

        SearchPhaseContext ctx = new MockSearchPhaseContext(1);

        compositeListener.onDFSPreQueryPhaseStart(ctx);
        assertEquals(totalListeners, dfsPreQueryPhaseStart.get());
        assertEquals(0, dfsPreQueryPhaseFailure.get());
        assertEquals(0, dfsPreQueryPhaseEnd.get());
        assertEquals(0, canMatchPhaseStart.get());
        assertEquals(0, canMatchPhaseFailure.get());
        assertEquals(0, canMatchPhaseEnd.get());
        assertEquals(0, queryPhaseStart.get());
        assertEquals(0, queryPhaseFailure.get());
        assertEquals(0, queryPhaseEnd.get());
        assertEquals(0, fetchPhaseStart.get());
        assertEquals(0, fetchPhaseFailure.get());
        assertEquals(0, fetchPhaseEnd.get());
        assertEquals(0, expandPhaseStart.get());
        assertEquals(0, expandPhaseFailure.get());
        assertEquals(0, expandPhaseEnd.get());

        compositeListener.onDFSPreQueryPhaseFailure(ctx);
        assertEquals(totalListeners, dfsPreQueryPhaseStart.get());
        assertEquals(totalListeners, dfsPreQueryPhaseFailure.get());
        assertEquals(0, dfsPreQueryPhaseEnd.get());
        assertEquals(0, canMatchPhaseStart.get());
        assertEquals(0, canMatchPhaseFailure.get());
        assertEquals(0, canMatchPhaseEnd.get());
        assertEquals(0, queryPhaseStart.get());
        assertEquals(0, queryPhaseFailure.get());
        assertEquals(0, queryPhaseEnd.get());
        assertEquals(0, fetchPhaseStart.get());
        assertEquals(0, fetchPhaseFailure.get());
        assertEquals(0, fetchPhaseEnd.get());
        assertEquals(0, expandPhaseStart.get());
        assertEquals(0, expandPhaseFailure.get());
        assertEquals(0, expandPhaseEnd.get());

        compositeListener.onDFSPreQueryPhaseEnd(ctx, timeInNanos.get());
        assertEquals(totalListeners, dfsPreQueryPhaseStart.get());
        assertEquals(totalListeners, dfsPreQueryPhaseFailure.get());
        assertEquals(totalListeners, dfsPreQueryPhaseEnd.get());
        assertEquals(0, canMatchPhaseStart.get());
        assertEquals(0, canMatchPhaseFailure.get());
        assertEquals(0, canMatchPhaseEnd.get());
        assertEquals(0, queryPhaseStart.get());
        assertEquals(0, queryPhaseFailure.get());
        assertEquals(0, queryPhaseEnd.get());
        assertEquals(0, fetchPhaseStart.get());
        assertEquals(0, fetchPhaseFailure.get());
        assertEquals(0, fetchPhaseEnd.get());
        assertEquals(0, expandPhaseStart.get());
        assertEquals(0, expandPhaseFailure.get());
        assertEquals(0, expandPhaseEnd.get());

        compositeListener.onCanMatchPhaseStart(ctx);
        assertEquals(totalListeners, dfsPreQueryPhaseStart.get());
        assertEquals(totalListeners, dfsPreQueryPhaseFailure.get());
        assertEquals(totalListeners, dfsPreQueryPhaseEnd.get());
        assertEquals(totalListeners, canMatchPhaseStart.get());
        assertEquals(0, canMatchPhaseFailure.get());
        assertEquals(0, canMatchPhaseEnd.get());
        assertEquals(0, queryPhaseStart.get());
        assertEquals(0, queryPhaseFailure.get());
        assertEquals(0, queryPhaseEnd.get());
        assertEquals(0, fetchPhaseStart.get());
        assertEquals(0, fetchPhaseFailure.get());
        assertEquals(0, fetchPhaseEnd.get());
        assertEquals(0, expandPhaseStart.get());
        assertEquals(0, expandPhaseFailure.get());
        assertEquals(0, expandPhaseEnd.get());

        compositeListener.onCanMatchPhaseFailure(ctx);
        assertEquals(totalListeners, dfsPreQueryPhaseStart.get());
        assertEquals(totalListeners, dfsPreQueryPhaseFailure.get());
        assertEquals(totalListeners, dfsPreQueryPhaseEnd.get());
        assertEquals(totalListeners, canMatchPhaseStart.get());
        assertEquals(totalListeners, canMatchPhaseFailure.get());
        assertEquals(0, canMatchPhaseEnd.get());
        assertEquals(0, queryPhaseStart.get());
        assertEquals(0, queryPhaseFailure.get());
        assertEquals(0, queryPhaseEnd.get());
        assertEquals(0, fetchPhaseStart.get());
        assertEquals(0, fetchPhaseFailure.get());
        assertEquals(0, fetchPhaseEnd.get());
        assertEquals(0, expandPhaseStart.get());
        assertEquals(0, expandPhaseFailure.get());
        assertEquals(0, expandPhaseEnd.get());

        compositeListener.onCanMatchPhaseEnd(ctx, timeInNanos.get());
        assertEquals(totalListeners, dfsPreQueryPhaseStart.get());
        assertEquals(totalListeners, dfsPreQueryPhaseFailure.get());
        assertEquals(totalListeners, dfsPreQueryPhaseEnd.get());
        assertEquals(totalListeners, canMatchPhaseStart.get());
        assertEquals(totalListeners, canMatchPhaseFailure.get());
        assertEquals(totalListeners, canMatchPhaseEnd.get());
        assertEquals(0, queryPhaseStart.get());
        assertEquals(0, queryPhaseFailure.get());
        assertEquals(0, queryPhaseEnd.get());
        assertEquals(0, fetchPhaseStart.get());
        assertEquals(0, fetchPhaseFailure.get());
        assertEquals(0, fetchPhaseEnd.get());
        assertEquals(0, expandPhaseStart.get());
        assertEquals(0, expandPhaseFailure.get());
        assertEquals(0, expandPhaseEnd.get());

        compositeListener.onQueryPhaseStart(ctx);
        assertEquals(totalListeners, dfsPreQueryPhaseStart.get());
        assertEquals(totalListeners, dfsPreQueryPhaseFailure.get());
        assertEquals(totalListeners, dfsPreQueryPhaseEnd.get());
        assertEquals(totalListeners, canMatchPhaseStart.get());
        assertEquals(totalListeners, canMatchPhaseFailure.get());
        assertEquals(totalListeners, canMatchPhaseEnd.get());
        assertEquals(totalListeners, queryPhaseStart.get());
        assertEquals(0, queryPhaseFailure.get());
        assertEquals(0, queryPhaseEnd.get());
        assertEquals(0, fetchPhaseStart.get());
        assertEquals(0, fetchPhaseFailure.get());
        assertEquals(0, fetchPhaseEnd.get());
        assertEquals(0, expandPhaseStart.get());
        assertEquals(0, expandPhaseFailure.get());
        assertEquals(0, expandPhaseEnd.get());

        compositeListener.onQueryPhaseFailure(ctx);
        assertEquals(totalListeners, dfsPreQueryPhaseStart.get());
        assertEquals(totalListeners, dfsPreQueryPhaseFailure.get());
        assertEquals(totalListeners, dfsPreQueryPhaseEnd.get());
        assertEquals(totalListeners, canMatchPhaseStart.get());
        assertEquals(totalListeners, canMatchPhaseFailure.get());
        assertEquals(totalListeners, canMatchPhaseEnd.get());
        assertEquals(totalListeners, queryPhaseStart.get());
        assertEquals(totalListeners, queryPhaseFailure.get());
        assertEquals(0, queryPhaseEnd.get());
        assertEquals(0, fetchPhaseStart.get());
        assertEquals(0, fetchPhaseFailure.get());
        assertEquals(0, fetchPhaseEnd.get());
        assertEquals(0, expandPhaseStart.get());
        assertEquals(0, expandPhaseFailure.get());
        assertEquals(0, expandPhaseEnd.get());

        compositeListener.onQueryPhaseEnd(ctx, timeInNanos.get());
        assertEquals(totalListeners, dfsPreQueryPhaseStart.get());
        assertEquals(totalListeners, dfsPreQueryPhaseFailure.get());
        assertEquals(totalListeners, dfsPreQueryPhaseEnd.get());
        assertEquals(totalListeners, canMatchPhaseStart.get());
        assertEquals(totalListeners, canMatchPhaseFailure.get());
        assertEquals(totalListeners, canMatchPhaseEnd.get());
        assertEquals(totalListeners, queryPhaseStart.get());
        assertEquals(totalListeners, queryPhaseFailure.get());
        assertEquals(totalListeners, queryPhaseEnd.get());
        assertEquals(0, fetchPhaseStart.get());
        assertEquals(0, fetchPhaseFailure.get());
        assertEquals(0, fetchPhaseEnd.get());
        assertEquals(0, expandPhaseStart.get());
        assertEquals(0, expandPhaseFailure.get());
        assertEquals(0, expandPhaseEnd.get());

        compositeListener.onFetchPhaseStart(ctx);
        assertEquals(totalListeners, dfsPreQueryPhaseStart.get());
        assertEquals(totalListeners, dfsPreQueryPhaseFailure.get());
        assertEquals(totalListeners, dfsPreQueryPhaseEnd.get());
        assertEquals(totalListeners, canMatchPhaseStart.get());
        assertEquals(totalListeners, canMatchPhaseFailure.get());
        assertEquals(totalListeners, canMatchPhaseEnd.get());
        assertEquals(totalListeners, queryPhaseStart.get());
        assertEquals(totalListeners, queryPhaseFailure.get());
        assertEquals(totalListeners, queryPhaseEnd.get());
        assertEquals(totalListeners, fetchPhaseStart.get());
        assertEquals(0, fetchPhaseFailure.get());
        assertEquals(0, fetchPhaseEnd.get());
        assertEquals(0, expandPhaseStart.get());
        assertEquals(0, expandPhaseFailure.get());
        assertEquals(0, expandPhaseEnd.get());

        compositeListener.onFetchPhaseFailure(ctx);
        assertEquals(totalListeners, dfsPreQueryPhaseStart.get());
        assertEquals(totalListeners, dfsPreQueryPhaseFailure.get());
        assertEquals(totalListeners, dfsPreQueryPhaseEnd.get());
        assertEquals(totalListeners, canMatchPhaseStart.get());
        assertEquals(totalListeners, canMatchPhaseFailure.get());
        assertEquals(totalListeners, canMatchPhaseEnd.get());
        assertEquals(totalListeners, queryPhaseStart.get());
        assertEquals(totalListeners, queryPhaseFailure.get());
        assertEquals(totalListeners, queryPhaseEnd.get());
        assertEquals(totalListeners, fetchPhaseStart.get());
        assertEquals(totalListeners, fetchPhaseFailure.get());
        assertEquals(0, fetchPhaseEnd.get());
        assertEquals(0, expandPhaseStart.get());
        assertEquals(0, expandPhaseFailure.get());
        assertEquals(0, expandPhaseEnd.get());

        compositeListener.onFetchPhaseEnd(ctx, timeInNanos.get());
        assertEquals(totalListeners, dfsPreQueryPhaseStart.get());
        assertEquals(totalListeners, dfsPreQueryPhaseFailure.get());
        assertEquals(totalListeners, dfsPreQueryPhaseEnd.get());
        assertEquals(totalListeners, canMatchPhaseStart.get());
        assertEquals(totalListeners, canMatchPhaseFailure.get());
        assertEquals(totalListeners, canMatchPhaseEnd.get());
        assertEquals(totalListeners, queryPhaseStart.get());
        assertEquals(totalListeners, queryPhaseFailure.get());
        assertEquals(totalListeners, queryPhaseEnd.get());
        assertEquals(totalListeners, fetchPhaseStart.get());
        assertEquals(totalListeners, fetchPhaseFailure.get());
        assertEquals(totalListeners, fetchPhaseEnd.get());
        assertEquals(0, expandPhaseStart.get());
        assertEquals(0, expandPhaseFailure.get());
        assertEquals(0, expandPhaseEnd.get());

        compositeListener.onExpandSearchPhaseStart(ctx);
        assertEquals(totalListeners, dfsPreQueryPhaseStart.get());
        assertEquals(totalListeners, dfsPreQueryPhaseFailure.get());
        assertEquals(totalListeners, dfsPreQueryPhaseEnd.get());
        assertEquals(totalListeners, canMatchPhaseStart.get());
        assertEquals(totalListeners, canMatchPhaseFailure.get());
        assertEquals(totalListeners, canMatchPhaseEnd.get());
        assertEquals(totalListeners, queryPhaseStart.get());
        assertEquals(totalListeners, queryPhaseFailure.get());
        assertEquals(totalListeners, queryPhaseEnd.get());
        assertEquals(totalListeners, fetchPhaseStart.get());
        assertEquals(totalListeners, fetchPhaseFailure.get());
        assertEquals(totalListeners, fetchPhaseEnd.get());
        assertEquals(totalListeners, expandPhaseStart.get());
        assertEquals(0, expandPhaseFailure.get());
        assertEquals(0, expandPhaseEnd.get());

        compositeListener.onExpandSearchPhaseFailure(ctx);
        assertEquals(totalListeners, dfsPreQueryPhaseStart.get());
        assertEquals(totalListeners, dfsPreQueryPhaseFailure.get());
        assertEquals(totalListeners, dfsPreQueryPhaseEnd.get());
        assertEquals(totalListeners, canMatchPhaseStart.get());
        assertEquals(totalListeners, canMatchPhaseFailure.get());
        assertEquals(totalListeners, canMatchPhaseEnd.get());
        assertEquals(totalListeners, queryPhaseStart.get());
        assertEquals(totalListeners, queryPhaseFailure.get());
        assertEquals(totalListeners, queryPhaseEnd.get());
        assertEquals(totalListeners, fetchPhaseStart.get());
        assertEquals(totalListeners, fetchPhaseFailure.get());
        assertEquals(totalListeners, fetchPhaseEnd.get());
        assertEquals(totalListeners, expandPhaseStart.get());
        assertEquals(totalListeners, expandPhaseFailure.get());
        assertEquals(0, expandPhaseEnd.get());

        compositeListener.onExpandSearchPhaseEnd(ctx, timeInNanos.get());
        assertEquals(totalListeners, dfsPreQueryPhaseStart.get());
        assertEquals(totalListeners, dfsPreQueryPhaseFailure.get());
        assertEquals(totalListeners, dfsPreQueryPhaseEnd.get());
        assertEquals(totalListeners, canMatchPhaseStart.get());
        assertEquals(totalListeners, canMatchPhaseFailure.get());
        assertEquals(totalListeners, canMatchPhaseEnd.get());
        assertEquals(totalListeners, queryPhaseStart.get());
        assertEquals(totalListeners, queryPhaseFailure.get());
        assertEquals(totalListeners, queryPhaseEnd.get());
        assertEquals(totalListeners, fetchPhaseStart.get());
        assertEquals(totalListeners, fetchPhaseFailure.get());
        assertEquals(totalListeners, fetchPhaseEnd.get());
        assertEquals(totalListeners, expandPhaseStart.get());
        assertEquals(totalListeners, expandPhaseFailure.get());
        assertEquals(totalListeners, expandPhaseEnd.get());
    }

}

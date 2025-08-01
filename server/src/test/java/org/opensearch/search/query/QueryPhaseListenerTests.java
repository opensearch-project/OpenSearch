/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.Query;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for QueryPhaseListener interface.
 */
public class QueryPhaseListenerTests extends OpenSearchTestCase {

    /**
     * Test implementation of QueryPhaseListener
     */
    private static class TestQueryPhaseListener implements QueryPhaseListener {
        private final AtomicInteger beforeCallCount = new AtomicInteger(0);
        private final AtomicInteger afterCallCount = new AtomicInteger(0);

        @Override
        public void beforeCollection(SearchContext searchContext) {
            beforeCallCount.incrementAndGet();
        }

        @Override
        public void afterCollection(SearchContext searchContext) {
            afterCallCount.incrementAndGet();
        }

        public int getBeforeCallCount() {
            return beforeCallCount.get();
        }

        public int getAfterCallCount() {
            return afterCallCount.get();
        }
    }

    public void testQueryPhaseListenerInterface() {
        TestQueryPhaseListener listener = new TestQueryPhaseListener();
        SearchContext mockContext = mock(SearchContext.class);

        // Initially no calls should be made
        assertEquals(0, listener.getBeforeCallCount());
        assertEquals(0, listener.getAfterCallCount());

        // Test beforeCollection
        listener.beforeCollection(mockContext);
        assertEquals(1, listener.getBeforeCallCount());
        assertEquals(0, listener.getAfterCallCount());

        // Test afterCollection
        listener.afterCollection(mockContext);
        assertEquals(1, listener.getBeforeCallCount());
        assertEquals(1, listener.getAfterCallCount());

        // Test multiple calls
        listener.beforeCollection(mockContext);
        listener.afterCollection(mockContext);
        assertEquals(2, listener.getBeforeCallCount());
        assertEquals(2, listener.getAfterCallCount());
    }

    public void testQueryPhaseListenerWithNullContext() {
        TestQueryPhaseListener listener = new TestQueryPhaseListener();

        // Should handle null context gracefully
        try {
            listener.beforeCollection(null);
            listener.afterCollection(null);
        } catch (Exception e) {
            fail("Extension should handle null context gracefully, but threw: " + e.getMessage());
        }

        assertEquals(1, listener.getBeforeCallCount());
        assertEquals(1, listener.getAfterCallCount());
    }

    /**
     * Test QueryPhaseSearcher implementation that includes listeners
     */
    private static class TestQueryPhaseSearcher extends QueryPhase.DefaultQueryPhaseSearcher {
        private final List<QueryPhaseListener> listeners;

        TestQueryPhaseSearcher(List<QueryPhaseListener> listeners) {
            this.listeners = listeners;
        }

        @Override
        public List<QueryPhaseListener> queryPhaseListeners() {
            return listeners == null ? super.queryPhaseListeners() : listeners;
        }
    }

    public void testQueryPhaseSearcherWithExtensions() {
        TestQueryPhaseListener listener1 = new TestQueryPhaseListener();
        TestQueryPhaseListener listener2 = new TestQueryPhaseListener();

        TestQueryPhaseSearcher searcher = new TestQueryPhaseSearcher(Arrays.asList(listener1, listener2));

        // Verify the listeners are registered
        List<QueryPhaseListener> listeners = searcher.queryPhaseListeners();
        assertEquals(2, listeners.size());
        assertEquals(listener1, listeners.get(0));
        assertEquals(listener2, listeners.get(1));

        // Verify default searcher has no listeners
        QueryPhase.DefaultQueryPhaseSearcher defaultSearcher = new QueryPhase.DefaultQueryPhaseSearcher();
        assertEquals(0, defaultSearcher.queryPhaseListeners().size());
    }

    public void testExtensionsCalledDuringSearchWithCollector() throws IOException {
        TestQueryPhaseListener listener1 = new TestQueryPhaseListener();
        TestQueryPhaseListener listener2 = new TestQueryPhaseListener();

        TestQueryPhaseSearcher searcher = new TestQueryPhaseSearcher(Arrays.asList(listener1, listener2));

        SearchContext searchContext = mock(SearchContext.class);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        Query query = mock(Query.class);
        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();

        // Verify listeners haven't been called yet
        assertEquals(0, listener1.getBeforeCallCount());
        assertEquals(0, listener1.getAfterCallCount());
        assertEquals(0, listener2.getBeforeCallCount());
        assertEquals(0, listener2.getAfterCallCount());

        try {
            searcher.searchWith(searchContext, indexSearcher, query, collectors, false, false);
            fail("Expected exception due to mock objects");
        } catch (Exception e) {
            // Expected - the search will fail with mock objects
            assertNotNull("Exception should not be null", e);
        }

        // Verify beforeCollection listeners were called, but afterCollection listeners were NOT called due to search failure
        assertEquals(1, listener1.getBeforeCallCount());
        assertEquals(0, listener1.getAfterCallCount()); // NOT called due to exception
        assertEquals(1, listener2.getBeforeCallCount());
        assertEquals(0, listener2.getAfterCallCount()); // NOT called due to exception
    }

    /**
     * Test implementation that throws exceptions
     */
    private static class ExceptionThrowingExtension implements QueryPhaseListener {

        @Override
        public void beforeCollection(SearchContext searchContext) {
            throw new RuntimeException("Exception in beforeCollection");
        }

        @Override
        public void afterCollection(SearchContext searchContext) {
            throw new RuntimeException("Exception in afterCollection");
        }
    }

    public void testExtensionExceptionHandling() throws IOException {
        ExceptionThrowingExtension badExtension = new ExceptionThrowingExtension();
        TestQueryPhaseSearcher searcher = new TestQueryPhaseSearcher(Arrays.asList(badExtension));

        SearchContext searchContext = mock(SearchContext.class);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        Query query = mock(Query.class);
        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();

        // Extension exceptions should now bubble up - plugins are responsible for their own exception handling
        try {
            searcher.searchWith(searchContext, indexSearcher, query, collectors, false, false);
            fail("Expected exception from listener");
        } catch (RuntimeException e) {
            // Expected - listener exceptions should bubble up
            assertEquals("Exception in beforeCollection", e.getMessage());
        }
    }

    public void testExtensionExceptionBubblesUp() throws IOException {
        ExceptionThrowingExtension badExtension = new ExceptionThrowingExtension();
        TestQueryPhaseSearcher searcher = new TestQueryPhaseSearcher(Arrays.asList(badExtension));

        SearchContext searchContext = mock(SearchContext.class);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        Query query = mock(Query.class);
        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();

        // Extension exceptions should bubble up instead of being caught
        try {
            searcher.searchWith(searchContext, indexSearcher, query, collectors, false, false);
            fail("Expected listener exception to bubble up");
        } catch (RuntimeException e) {
            // Expected - listener exceptions should bubble up to the caller
            assertEquals("Exception in beforeCollection", e.getMessage());
        }
    }

    /**
     * Test listener that verifies search context state during execution
     */
    private static class StateVerifyingExtension implements QueryPhaseListener {
        private final AtomicBoolean beforeCalled = new AtomicBoolean(false);
        private final AtomicBoolean afterCalled = new AtomicBoolean(false);
        private final AtomicReference<SearchContext> beforeContext = new AtomicReference<>();
        private final AtomicReference<SearchContext> afterContext = new AtomicReference<>();
        private final AtomicBoolean queryResultAvailableInAfter = new AtomicBoolean(false);

        @Override
        public void beforeCollection(SearchContext searchContext) {
            beforeCalled.set(true);
            beforeContext.set(searchContext);
            // At this point, query should be available but not results
            assertNotNull("SearchContext should not be null in beforeCollection", searchContext);
            assertNotNull("Query should be available in beforeCollection", searchContext.query());
        }

        @Override
        public void afterCollection(SearchContext searchContext) {
            afterCalled.set(true);
            afterContext.set(searchContext);
            // At this point, both query and results should be available
            assertNotNull("SearchContext should not be null in afterCollection", searchContext);
            assertNotNull("Query should be available in afterCollection", searchContext.query());
            if (searchContext.queryResult() != null) {
                queryResultAvailableInAfter.set(true);
            }
        }

        public boolean wasBeforeCalled() {
            return beforeCalled.get();
        }

        public boolean wasAfterCalled() {
            return afterCalled.get();
        }

        public boolean wasQueryResultAvailableInAfter() {
            return queryResultAvailableInAfter.get();
        }

        public SearchContext getBeforeContext() {
            return beforeContext.get();
        }

        public SearchContext getAfterContext() {
            return afterContext.get();
        }
    }

    public void testBeforeAndAfterScoreCollectionWorkAsExtensionPoints() throws IOException {
        StateVerifyingExtension stateExtension = new StateVerifyingExtension();
        TestQueryPhaseSearcher searcher = new TestQueryPhaseSearcher(Arrays.asList(stateExtension));

        SearchContext searchContext = mock(SearchContext.class);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        Query query = mock(Query.class);
        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();

        // Setup mocks - don't mock final classes like QuerySearchResult
        when(searchContext.query()).thenReturn(query);
        when(searchContext.searcher()).thenReturn(indexSearcher);
        // queryResult() will return null from the mock, which is fine for our test

        // Initially, listener should not have been called
        assertFalse("beforeCollection should not be called yet", stateExtension.wasBeforeCalled());
        assertFalse("afterCollection should not be called yet", stateExtension.wasAfterCalled());

        try {
            searcher.searchWith(searchContext, indexSearcher, query, collectors, false, false);
            fail("Expected exception due to mock objects");
        } catch (Exception e) {
            // Expected - the search will fail with mock objects
        }

        // Verify beforeCollection was called, but afterCollection was NOT called due to search failure
        assertTrue("beforeCollection should have been called", stateExtension.wasBeforeCalled());
        assertFalse("afterCollection should NOT have been called due to exception", stateExtension.wasAfterCalled());

        // Verify the context was passed to beforeCollection method
        assertSame("Context should be the one we provided", searchContext, stateExtension.getBeforeContext());
    }

    /**
     * Test listener that modifies search context state
     */
    private static class ContextModifyingExtension implements QueryPhaseListener {
        private final String tag;
        private final List<String> executionOrder;

        ContextModifyingExtension(String tag, List<String> executionOrder) {
            this.tag = tag;
            this.executionOrder = executionOrder;
        }

        @Override
        public void beforeCollection(SearchContext searchContext) {
            executionOrder.add("before-" + tag);
        }

        @Override
        public void afterCollection(SearchContext searchContext) {
            executionOrder.add("after-" + tag);
        }
    }

    public void testMultipleExtensionsExecutionOrder() throws IOException {
        List<String> executionOrder = new ArrayList<>();

        ContextModifyingExtension ext1 = new ContextModifyingExtension("ext1", executionOrder);
        ContextModifyingExtension ext2 = new ContextModifyingExtension("ext2", executionOrder);
        ContextModifyingExtension ext3 = new ContextModifyingExtension("ext3", executionOrder);

        TestQueryPhaseSearcher searcher = new TestQueryPhaseSearcher(Arrays.asList(ext1, ext2, ext3));

        SearchContext searchContext = mock(SearchContext.class);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        Query query = mock(Query.class);
        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();

        try {
            searcher.searchWith(searchContext, indexSearcher, query, collectors, false, false);
            fail("Expected exception due to mock objects");
        } catch (Exception e) {
            // Expected
        }

        // Verify execution order - only beforeCollection listeners should have been called due to search failure
        assertEquals("Should have 3 execution entries (only before listeners)", 3, executionOrder.size());
        assertEquals("First should be before-ext1", "before-ext1", executionOrder.get(0));
        assertEquals("Second should be before-ext2", "before-ext2", executionOrder.get(1));
        assertEquals("Third should be before-ext3", "before-ext3", executionOrder.get(2));
        // The actual search fails here, so no afterCollection listeners are called
    }

    /**
     * Test that verifies listeners work with ConcurrentQueryPhaseSearcher
     */
    public void testExtensionsWithConcurrentQueryPhaseSearcher() throws IOException {
        TestQueryPhaseListener listener = new TestQueryPhaseListener();

        ConcurrentQueryPhaseSearcher concurrentSearcher = new ConcurrentQueryPhaseSearcher() {
            @Override
            public List<QueryPhaseListener> queryPhaseListeners() {
                return Arrays.asList(listener);
            }
        };

        SearchContext searchContext = mock(SearchContext.class);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        Query query = mock(Query.class);
        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();

        try {
            concurrentSearcher.searchWith(searchContext, indexSearcher, query, collectors, false, false);
            fail("Expected exception due to mock objects");
        } catch (Exception e) {
            // Expected
        }

        // Verify listener was called with concurrent searcher - only beforeCollection due to search failure
        assertEquals("Extension should be called before score collection", 1, listener.getBeforeCallCount());
        assertEquals("Extension should NOT be called after score collection due to exception", 0, listener.getAfterCallCount());
    }

    public void testNoExtensionsRegistered() throws IOException {
        // Test with no listeners - should work normally
        TestQueryPhaseSearcher searcher = new TestQueryPhaseSearcher(new ArrayList<>());

        SearchContext searchContext = mock(SearchContext.class);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        Query query = mock(Query.class);
        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();

        try {
            searcher.searchWith(searchContext, indexSearcher, query, collectors, false, false);
            fail("Expected exception due to mock objects");
        } catch (Exception e) {
            // Expected - should fail due to mocks, not due to listener handling
            assertNotNull(e);
        }
    }

    public void testNullExtensionsList() throws IOException {
        // Test with null listeners list - should work normally
        TestQueryPhaseSearcher searcher = new TestQueryPhaseSearcher(null);

        SearchContext searchContext = mock(SearchContext.class);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        Query query = mock(Query.class);
        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();

        try {
            searcher.searchWith(searchContext, indexSearcher, query, collectors, false, false);
            fail("Expected exception due to mock objects");
        } catch (Exception e) {
            // Expected - should fail due to mocks, not due to listener handling
            assertNotNull(e);
        }
    }

    public void testDefaultQueryPhaseSearcherHasNoExtensions() {
        QueryPhase.DefaultQueryPhaseSearcher defaultSearcher = new QueryPhase.DefaultQueryPhaseSearcher();
        List<QueryPhaseListener> listeners = defaultSearcher.queryPhaseListeners();

        assertNotNull("Extensions list should not be null", listeners);
        assertTrue("Extensions list should be empty", listeners.isEmpty());
        assertEquals("Extensions list should have size 0", 0, listeners.size());
    }

    public void testConcurrentQueryPhaseSearcherHasNoExtensionsByDefault() {
        ConcurrentQueryPhaseSearcher concurrentSearcher = new ConcurrentQueryPhaseSearcher();
        List<QueryPhaseListener> listeners = concurrentSearcher.queryPhaseListeners();

        assertNotNull("Extensions list should not be null", listeners);
        assertTrue("Extensions list should be empty", listeners.isEmpty());
        assertEquals("Extensions list should have size 0", 0, listeners.size());
    }

    public void testQueryPhaseSearcherInterfaceDefault() {
        // Test the default implementation in the interface
        QueryPhaseSearcher testSearcher = new QueryPhaseSearcher() {
            @Override
            public boolean searchWith(
                SearchContext searchContext,
                ContextIndexSearcher searcher,
                Query query,
                LinkedList<QueryCollectorContext> collectors,
                boolean hasFilterCollector,
                boolean hasTimeout
            ) throws IOException {
                return false;
            }
        };

        List<QueryPhaseListener> listeners = testSearcher.queryPhaseListeners();
        assertNotNull("Extensions list should not be null", listeners);
        assertTrue("Extensions list should be empty by default", listeners.isEmpty());
        assertEquals("Extensions list should have size 0", 0, listeners.size());
    }

    public void testExtensionReceivesCorrectSearchContext() throws IOException {
        final AtomicReference<SearchContext> capturedBeforeContext = new AtomicReference<>();
        final AtomicReference<SearchContext> capturedAfterContext = new AtomicReference<>();

        QueryPhaseListener capturingExtension = new QueryPhaseListener() {
            @Override
            public void beforeCollection(SearchContext searchContext) {
                capturedBeforeContext.set(searchContext);
            }

            @Override
            public void afterCollection(SearchContext searchContext) {
                capturedAfterContext.set(searchContext);
            }
        };

        TestQueryPhaseSearcher searcher = new TestQueryPhaseSearcher(Arrays.asList(capturingExtension));

        SearchContext searchContext = mock(SearchContext.class);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        Query query = mock(Query.class);
        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();

        // Setup mock to return expected values
        when(searchContext.query()).thenReturn(query);
        when(searchContext.searcher()).thenReturn(indexSearcher);

        try {
            searcher.searchWith(searchContext, indexSearcher, query, collectors, false, false);
            fail("Expected exception due to mock objects");
        } catch (Exception e) {
            // Expected
        }

        // Verify the listener received the correct search context for beforeCollection, but not for afterCollection due to exception
        assertNotNull("beforeCollection should have received a context", capturedBeforeContext.get());
        assertNull("afterCollection should NOT have received a context due to exception", capturedAfterContext.get());
        assertSame("Extension should receive the provided search context", searchContext, capturedBeforeContext.get());
    }

    /**
     * Test listener that tracks detailed execution state
     */
    private static class DetailedTrackingExtension implements QueryPhaseListener {
        protected final AtomicInteger beforeCount = new AtomicInteger(0);
        protected final AtomicInteger afterCount = new AtomicInteger(0);
        protected final AtomicReference<Throwable> lastException = new AtomicReference<>();
        private final boolean shouldThrow;
        private final String listenerName;

        DetailedTrackingExtension(String name, boolean shouldThrow) {
            this.listenerName = name;
            this.shouldThrow = shouldThrow;
        }

        @Override
        public void beforeCollection(SearchContext searchContext) {
            beforeCount.incrementAndGet();
            if (shouldThrow) {
                RuntimeException ex = new RuntimeException("Test exception from " + listenerName + " beforeCollection");
                lastException.set(ex);
                throw ex;
            }
        }

        @Override
        public void afterCollection(SearchContext searchContext) {
            afterCount.incrementAndGet();
            if (shouldThrow) {
                RuntimeException ex = new RuntimeException("Test exception from " + listenerName + " afterCollection");
                lastException.set(ex);
                throw ex;
            }
        }

        public int getBeforeCount() {
            return beforeCount.get();
        }

        public int getAfterCount() {
            return afterCount.get();
        }

        public Throwable getLastException() {
            return lastException.get();
        }

        public String getName() {
            return listenerName;
        }
    }

}

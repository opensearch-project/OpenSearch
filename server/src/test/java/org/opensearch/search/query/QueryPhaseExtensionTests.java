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
 * Unit tests for QueryPhaseExtension interface.
 */
public class QueryPhaseExtensionTests extends OpenSearchTestCase {

    /**
     * Test implementation of QueryPhaseExtension
     */
    private static class TestQueryPhaseExtension implements QueryPhaseExtension {
        private final AtomicInteger beforeCallCount = new AtomicInteger(0);
        private final AtomicInteger afterCallCount = new AtomicInteger(0);

        @Override
        public void beforeScoreCollection(SearchContext searchContext) {
            beforeCallCount.incrementAndGet();
        }

        @Override
        public void afterScoreCollection(SearchContext searchContext) {
            afterCallCount.incrementAndGet();
        }

        public int getBeforeCallCount() {
            return beforeCallCount.get();
        }

        public int getAfterCallCount() {
            return afterCallCount.get();
        }
    }

    public void testQueryPhaseExtensionInterface() {
        TestQueryPhaseExtension extension = new TestQueryPhaseExtension();
        SearchContext mockContext = mock(SearchContext.class);

        // Initially no calls should be made
        assertEquals(0, extension.getBeforeCallCount());
        assertEquals(0, extension.getAfterCallCount());

        // Test beforeScoreCollection
        extension.beforeScoreCollection(mockContext);
        assertEquals(1, extension.getBeforeCallCount());
        assertEquals(0, extension.getAfterCallCount());

        // Test afterScoreCollection
        extension.afterScoreCollection(mockContext);
        assertEquals(1, extension.getBeforeCallCount());
        assertEquals(1, extension.getAfterCallCount());

        // Test multiple calls
        extension.beforeScoreCollection(mockContext);
        extension.afterScoreCollection(mockContext);
        assertEquals(2, extension.getBeforeCallCount());
        assertEquals(2, extension.getAfterCallCount());
    }

    public void testQueryPhaseExtensionWithNullContext() {
        TestQueryPhaseExtension extension = new TestQueryPhaseExtension();

        // Should handle null context gracefully
        try {
            extension.beforeScoreCollection(null);
            extension.afterScoreCollection(null);
        } catch (Exception e) {
            fail("Extension should handle null context gracefully, but threw: " + e.getMessage());
        }

        assertEquals(1, extension.getBeforeCallCount());
        assertEquals(1, extension.getAfterCallCount());
    }

    /**
     * Test QueryPhaseSearcher implementation that includes extensions
     */
    private static class TestQueryPhaseSearcher extends QueryPhase.DefaultQueryPhaseSearcher {
        private final List<QueryPhaseExtension> extensions;

        TestQueryPhaseSearcher(List<QueryPhaseExtension> extensions) {
            this.extensions = extensions;
        }

        @Override
        public List<QueryPhaseExtension> queryPhaseExtensions() {
            return extensions;
        }
    }

    public void testQueryPhaseSearcherWithExtensions() {
        TestQueryPhaseExtension extension1 = new TestQueryPhaseExtension();
        TestQueryPhaseExtension extension2 = new TestQueryPhaseExtension();

        TestQueryPhaseSearcher searcher = new TestQueryPhaseSearcher(Arrays.asList(extension1, extension2));

        // Verify the extensions are registered
        List<QueryPhaseExtension> extensions = searcher.queryPhaseExtensions();
        assertEquals(2, extensions.size());
        assertEquals(extension1, extensions.get(0));
        assertEquals(extension2, extensions.get(1));

        // Verify default searcher has no extensions
        QueryPhase.DefaultQueryPhaseSearcher defaultSearcher = new QueryPhase.DefaultQueryPhaseSearcher();
        assertEquals(0, defaultSearcher.queryPhaseExtensions().size());
    }

    public void testExtensionsCalledDuringSearchWithCollector() throws IOException {
        TestQueryPhaseExtension extension1 = new TestQueryPhaseExtension();
        TestQueryPhaseExtension extension2 = new TestQueryPhaseExtension();

        TestQueryPhaseSearcher searcher = new TestQueryPhaseSearcher(Arrays.asList(extension1, extension2));

        SearchContext searchContext = mock(SearchContext.class);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        Query query = mock(Query.class);
        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();

        // Verify extensions haven't been called yet
        assertEquals(0, extension1.getBeforeCallCount());
        assertEquals(0, extension1.getAfterCallCount());
        assertEquals(0, extension2.getBeforeCallCount());
        assertEquals(0, extension2.getAfterCallCount());

        try {
            searcher.searchWith(searchContext, indexSearcher, query, collectors, false, false);
            fail("Expected exception due to mock objects");
        } catch (Exception e) {
            // Expected - the search will fail with mock objects, but extensions should still be called
            assertNotNull("Exception should not be null", e);
        }

        // Verify all extensions were called despite the search failure
        assertEquals(1, extension1.getBeforeCallCount());
        assertEquals(1, extension1.getAfterCallCount());
        assertEquals(1, extension2.getBeforeCallCount());
        assertEquals(1, extension2.getAfterCallCount());
    }

    /**
     * Test implementation that throws exceptions
     */
    private static class ExceptionThrowingExtension implements QueryPhaseExtension {

        @Override
        public void beforeScoreCollection(SearchContext searchContext) {
            throw new RuntimeException("Exception in beforeScoreCollection");
        }

        @Override
        public void afterScoreCollection(SearchContext searchContext) {
            throw new RuntimeException("Exception in afterScoreCollection");
        }
    }

    public void testExtensionExceptionHandling() throws IOException {
        TestQueryPhaseExtension goodExtension = new TestQueryPhaseExtension();
        ExceptionThrowingExtension badExtension = new ExceptionThrowingExtension();

        TestQueryPhaseSearcher searcher = new TestQueryPhaseSearcher(Arrays.asList(goodExtension, badExtension));

        SearchContext searchContext = mock(SearchContext.class);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        Query query = mock(Query.class);
        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();

        // The search should continue despite extension throwing exceptions
        try {
            searcher.searchWith(searchContext, indexSearcher, query, collectors, false, false);
            fail("Expected exception due to mock objects");
        } catch (Exception e) {
            // Expected - the search will fail with mock objects, but should not be due to extension failures
            assertNotNull("Exception should not be null", e);
            assertFalse(
                "Exception should not be QueryPhaseExecutionException from extension",
                e instanceof QueryPhaseExecutionException && e.getMessage().contains("extension")
            );
        }

        // The good extension should still have been called
        assertEquals(1, goodExtension.getBeforeCallCount());
        assertEquals(1, goodExtension.getAfterCallCount());
    }

    public void testExtensionGracefulErrorHandling() throws IOException {
        TestQueryPhaseExtension goodExtension = new TestQueryPhaseExtension();
        ExceptionThrowingExtension badExtension = new ExceptionThrowingExtension();

        TestQueryPhaseSearcher searcher = new TestQueryPhaseSearcher(Arrays.asList(goodExtension, badExtension));

        SearchContext searchContext = mock(SearchContext.class);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        Query query = mock(Query.class);
        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();

        // The search should continue gracefully despite extension throwing exceptions
        try {
            searcher.searchWith(searchContext, indexSearcher, query, collectors, false, false);
            fail("Expected exception due to mock objects");
        } catch (Exception e) {
            // Expected - the search will fail with mock objects, but extensions are handled gracefully
            assertNotNull("Exception should not be null", e);
            // Verify it's not an extension failure but rather the expected mock failure
            assertFalse(
                "Exception should not be from extension failure",
                e instanceof QueryPhaseExecutionException && e.getMessage().contains("extension")
            );
        }

        // Both extensions should have been called despite the bad extension throwing exceptions
        assertEquals(1, goodExtension.getBeforeCallCount());
        assertEquals(1, goodExtension.getAfterCallCount());
    }

    /**
     * Test extension that verifies search context state during execution
     */
    private static class StateVerifyingExtension implements QueryPhaseExtension {
        private final AtomicBoolean beforeCalled = new AtomicBoolean(false);
        private final AtomicBoolean afterCalled = new AtomicBoolean(false);
        private final AtomicReference<SearchContext> beforeContext = new AtomicReference<>();
        private final AtomicReference<SearchContext> afterContext = new AtomicReference<>();
        private final AtomicBoolean queryResultAvailableInAfter = new AtomicBoolean(false);

        @Override
        public void beforeScoreCollection(SearchContext searchContext) {
            beforeCalled.set(true);
            beforeContext.set(searchContext);
            // At this point, query should be available but not results
            assertNotNull("SearchContext should not be null in beforeScoreCollection", searchContext);
            assertNotNull("Query should be available in beforeScoreCollection", searchContext.query());
        }

        @Override
        public void afterScoreCollection(SearchContext searchContext) {
            afterCalled.set(true);
            afterContext.set(searchContext);
            // At this point, both query and results should be available
            assertNotNull("SearchContext should not be null in afterScoreCollection", searchContext);
            assertNotNull("Query should be available in afterScoreCollection", searchContext.query());
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

        // Initially, extension should not have been called
        assertFalse("beforeScoreCollection should not be called yet", stateExtension.wasBeforeCalled());
        assertFalse("afterScoreCollection should not be called yet", stateExtension.wasAfterCalled());

        try {
            searcher.searchWith(searchContext, indexSearcher, query, collectors, false, false);
            fail("Expected exception due to mock objects");
        } catch (Exception e) {
            // Expected - the search will fail with mock objects
        }

        // Verify both methods were called
        assertTrue("beforeScoreCollection should have been called", stateExtension.wasBeforeCalled());
        assertTrue("afterScoreCollection should have been called", stateExtension.wasAfterCalled());

        // Verify the same context was passed to both methods
        assertSame("Same context should be passed to both methods", stateExtension.getBeforeContext(), stateExtension.getAfterContext());
        assertSame("Context should be the one we provided", searchContext, stateExtension.getBeforeContext());
    }

    /**
     * Test extension that modifies search context state
     */
    private static class ContextModifyingExtension implements QueryPhaseExtension {
        private final String tag;
        private final List<String> executionOrder;

        ContextModifyingExtension(String tag, List<String> executionOrder) {
            this.tag = tag;
            this.executionOrder = executionOrder;
        }

        @Override
        public void beforeScoreCollection(SearchContext searchContext) {
            executionOrder.add("before-" + tag);
        }

        @Override
        public void afterScoreCollection(SearchContext searchContext) {
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

        // Verify execution order
        assertEquals("Should have 6 execution entries", 6, executionOrder.size());
        assertEquals("First should be before-ext1", "before-ext1", executionOrder.get(0));
        assertEquals("Second should be before-ext2", "before-ext2", executionOrder.get(1));
        assertEquals("Third should be before-ext3", "before-ext3", executionOrder.get(2));
        // The actual search happens here
        assertEquals("Fourth should be after-ext1", "after-ext1", executionOrder.get(3));
        assertEquals("Fifth should be after-ext2", "after-ext2", executionOrder.get(4));
        assertEquals("Sixth should be after-ext3", "after-ext3", executionOrder.get(5));
    }

    /**
     * Test that verifies extensions work with ConcurrentQueryPhaseSearcher
     */
    public void testExtensionsWithConcurrentQueryPhaseSearcher() throws IOException {
        TestQueryPhaseExtension extension = new TestQueryPhaseExtension();

        ConcurrentQueryPhaseSearcher concurrentSearcher = new ConcurrentQueryPhaseSearcher() {
            @Override
            public List<QueryPhaseExtension> queryPhaseExtensions() {
                return Arrays.asList(extension);
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

        // Verify extension was called with concurrent searcher
        assertEquals("Extension should be called before score collection", 1, extension.getBeforeCallCount());
        assertEquals("Extension should be called after score collection", 1, extension.getAfterCallCount());
    }

    public void testNoExtensionsRegistered() throws IOException {
        // Test with no extensions - should work normally
        TestQueryPhaseSearcher searcher = new TestQueryPhaseSearcher(new ArrayList<>());

        SearchContext searchContext = mock(SearchContext.class);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        Query query = mock(Query.class);
        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();

        try {
            searcher.searchWith(searchContext, indexSearcher, query, collectors, false, false);
            fail("Expected exception due to mock objects");
        } catch (Exception e) {
            // Expected - should fail due to mocks, not due to extension handling
            assertNotNull(e);
        }
    }

    public void testExtensionReceivesCorrectSearchContext() throws IOException {
        final AtomicReference<SearchContext> capturedBeforeContext = new AtomicReference<>();
        final AtomicReference<SearchContext> capturedAfterContext = new AtomicReference<>();

        QueryPhaseExtension capturingExtension = new QueryPhaseExtension() {
            @Override
            public void beforeScoreCollection(SearchContext searchContext) {
                capturedBeforeContext.set(searchContext);
            }

            @Override
            public void afterScoreCollection(SearchContext searchContext) {
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

        // Verify the extension received the correct search context
        assertNotNull("beforeScoreCollection should have received a context", capturedBeforeContext.get());
        assertNotNull("afterScoreCollection should have received a context", capturedAfterContext.get());
        assertSame("Both methods should receive the same context", capturedBeforeContext.get(), capturedAfterContext.get());
        assertSame("Extension should receive the provided search context", searchContext, capturedBeforeContext.get());
    }
}

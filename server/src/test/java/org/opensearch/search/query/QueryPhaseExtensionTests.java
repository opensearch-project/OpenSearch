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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;

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
        private final boolean failFast;

        ExceptionThrowingExtension(boolean failFast) {
            this.failFast = failFast;
        }

        @Override
        public void beforeScoreCollection(SearchContext searchContext) {
            throw new RuntimeException("Exception in beforeScoreCollection");
        }

        @Override
        public void afterScoreCollection(SearchContext searchContext) {
            throw new RuntimeException("Exception in afterScoreCollection");
        }

        @Override
        public boolean failOnError() {
            return failFast;
        }
    }

    public void testExtensionExceptionHandling() throws IOException {
        TestQueryPhaseExtension goodExtension = new TestQueryPhaseExtension();
        ExceptionThrowingExtension badExtension = new ExceptionThrowingExtension(false); // failOnError = false

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
            // The exception should not be from our extension since failOnError = false
            assertFalse("Exception should not be QueryPhaseExecutionException", e instanceof QueryPhaseExecutionException);
        }

        // The good extension should still have been called
        assertEquals(1, goodExtension.getBeforeCallCount());
        assertEquals(1, goodExtension.getAfterCallCount());
    }

    public void testExtensionFailFastBehavior() throws IOException {
        TestQueryPhaseExtension goodExtension = new TestQueryPhaseExtension();
        ExceptionThrowingExtension failFastExtension = new ExceptionThrowingExtension(true); // failOnError = true

        TestQueryPhaseSearcher searcher = new TestQueryPhaseSearcher(Arrays.asList(goodExtension, failFastExtension));

        SearchContext searchContext = mock(SearchContext.class);
        ContextIndexSearcher indexSearcher = mock(ContextIndexSearcher.class);
        Query query = mock(Query.class);
        LinkedList<QueryCollectorContext> collectors = new LinkedList<>();

        // The search should fail when failOnError = true
        try {
            searcher.searchWith(searchContext, indexSearcher, query, collectors, false, false);
            fail("Expected QueryPhaseExecutionException");
        } catch (QueryPhaseExecutionException e) {
            // Expected
            assertTrue(e.getMessage().contains("Failed to execute beforeScoreCollection extension"));
            assertTrue(e.getMessage().contains("ExceptionThrowingExtension"));
        }

        // The good extension should have been called before the failure
        assertEquals(1, goodExtension.getBeforeCallCount());
        // afterScoreCollection should not be called due to early failure
        assertEquals(0, goodExtension.getAfterCallCount());
    }
}

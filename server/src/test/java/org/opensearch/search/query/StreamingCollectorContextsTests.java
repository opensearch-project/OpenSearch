/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Sort;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests streaming collector context instantiation.
 */
public class StreamingCollectorContextsTests extends OpenSearchTestCase {

    private SearchContext mockSearchContext;
    private CircuitBreaker mockCircuitBreaker;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockSearchContext = mock(SearchContext.class);
        mockCircuitBreaker = mock(CircuitBreaker.class);
        when(mockSearchContext.size()).thenReturn(10);
    }

    public void testStreamingUnsortedCollectorContextInstantiation() throws IOException {
        StreamingUnsortedCollectorContext context1 = new StreamingUnsortedCollectorContext("test_unsorted", 10, mockSearchContext);
        assertNotNull(context1);
        assertEquals(10, context1.numHits());

        Collector collector1 = context1.create(null);
        assertNotNull(collector1);

        StreamingUnsortedCollectorContext context2 = new StreamingUnsortedCollectorContext(
            "test_unsorted_breaker",
            20,
            mockSearchContext,
            mockCircuitBreaker
        );
        assertNotNull(context2);
        assertEquals(20, context2.numHits());

        Collector collector2 = context2.create(null);
        assertNotNull(collector2);
    }

    public void testStreamingScoredUnsortedCollectorContextInstantiation() throws IOException {
        StreamingScoredUnsortedCollectorContext context1 = new StreamingScoredUnsortedCollectorContext(
            "test_scored_unsorted",
            10,
            mockSearchContext
        );
        assertNotNull(context1);
        assertEquals(10, context1.numHits());

        Collector collector1 = context1.create(null);
        assertNotNull(collector1);

        StreamingScoredUnsortedCollectorContext context2 = new StreamingScoredUnsortedCollectorContext(
            "test_scored_unsorted_breaker",
            20,
            mockSearchContext,
            mockCircuitBreaker
        );
        assertNotNull(context2);
        assertEquals(20, context2.numHits());

        Collector collector2 = context2.create(null);
        assertNotNull(collector2);
    }

    public void testStreamingSortedCollectorContextInstantiation() throws IOException {
        StreamingSortedCollectorContext context1 = new StreamingSortedCollectorContext("test_sorted", 10, mockSearchContext);
        assertNotNull(context1);
        assertEquals(10, context1.numHits());

        Collector collector1 = context1.create(null);
        assertNotNull(collector1);

        StreamingSortedCollectorContext context2 = new StreamingSortedCollectorContext(
            "test_sorted_with_sort",
            15,
            mockSearchContext,
            Sort.RELEVANCE
        );
        assertNotNull(context2);
        assertEquals(15, context2.numHits());

        Collector collector2 = context2.create(null);
        assertNotNull(collector2);

        StreamingSortedCollectorContext context3 = new StreamingSortedCollectorContext(
            "test_sorted_full",
            20,
            mockSearchContext,
            Sort.RELEVANCE,
            mockCircuitBreaker
        );
        assertNotNull(context3);
        assertEquals(20, context3.numHits());

        Collector collector3 = context3.create(null);
        assertNotNull(collector3);

        StreamingSortedCollectorContext context4 = new StreamingSortedCollectorContext(
            "test_sorted_breaker",
            25,
            mockSearchContext,
            mockCircuitBreaker
        );
        assertNotNull(context4);
        assertEquals(25, context4.numHits());

        Collector collector4 = context4.create(null);
        assertNotNull(collector4);
    }
}

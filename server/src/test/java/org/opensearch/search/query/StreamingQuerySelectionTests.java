/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.Sort;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.search.query.StreamingSearchMode;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests streaming collector selection.
 */
public class StreamingQuerySelectionTests extends OpenSearchTestCase {

    private SearchContext mockSearchContext;
    private CircuitBreaker mockCircuitBreaker;
    private BigArrays mockBigArrays;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockSearchContext = mock(SearchContext.class);
        mockCircuitBreaker = new NoopCircuitBreaker(CircuitBreaker.REQUEST);
        mockBigArrays = mock(BigArrays.class);
        when(mockSearchContext.size()).thenReturn(10);
        when(mockSearchContext.bigArrays()).thenReturn(mockBigArrays);
    }

    public void testNoScoringModeSelectsStreamingUnsortedCollector() throws IOException {
        when(mockSearchContext.isStreamingSearch()).thenReturn(true);
        when(mockSearchContext.getStreamingMode()).thenReturn(StreamingSearchMode.NO_SCORING);

        TopDocsCollectorContext context = TopDocsCollectorContext.createStreamingTopDocsCollectorContext(
            mockSearchContext,
            false
        );

        assertNotNull(context);
        assertTrue(context instanceof StreamingUnsortedCollectorContext);
        assertEquals(10, context.numHits());
    }

    public void testScoredUnsortedModeSelectsStreamingScoredUnsortedCollector() throws IOException {
        when(mockSearchContext.isStreamingSearch()).thenReturn(true);
        when(mockSearchContext.getStreamingMode()).thenReturn(StreamingSearchMode.SCORED_UNSORTED);

        TopDocsCollectorContext context = TopDocsCollectorContext.createStreamingTopDocsCollectorContext(
            mockSearchContext,
            false
        );

        assertNotNull(context);
        assertTrue(context instanceof StreamingScoredUnsortedCollectorContext);
        assertEquals(10, context.numHits());
    }

    public void testScoredSortedModeSelectsStreamingSortedCollector() throws IOException {
        when(mockSearchContext.isStreamingSearch()).thenReturn(true);
        when(mockSearchContext.getStreamingMode()).thenReturn(StreamingSearchMode.SCORED_SORTED);
        when(mockSearchContext.sort()).thenReturn(null);

        TopDocsCollectorContext context = TopDocsCollectorContext.createStreamingTopDocsCollectorContext(
            mockSearchContext,
            false
        );

        assertNotNull(context);
        assertTrue(context instanceof StreamingSortedCollectorContext);
        assertEquals(10, context.numHits());
    }

    public void testScoredSortedModeWithCustomSort() throws IOException {
        when(mockSearchContext.isStreamingSearch()).thenReturn(true);
        when(mockSearchContext.getStreamingMode()).thenReturn(StreamingSearchMode.SCORED_SORTED);

        SortAndFormats sortAndFormats = new SortAndFormats(
            Sort.INDEXORDER,
            new org.opensearch.search.DocValueFormat[] { org.opensearch.search.DocValueFormat.RAW }
        );
        when(mockSearchContext.sort()).thenReturn(sortAndFormats);

        TopDocsCollectorContext context = TopDocsCollectorContext.createStreamingTopDocsCollectorContext(
            mockSearchContext,
            false
        );

        assertNotNull(context);
        assertTrue(context instanceof StreamingSortedCollectorContext);
        assertEquals(10, context.numHits());
    }

    public void testNullStreamingModeThrowsException() {
        when(mockSearchContext.isStreamingSearch()).thenReturn(true);
        when(mockSearchContext.getStreamingMode()).thenReturn(null);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TopDocsCollectorContext.createStreamingTopDocsCollectorContext(mockSearchContext, false)
        );

        assertEquals("Streaming mode must be set for streaming collectors", exception.getMessage());
    }

    public void testFallbackToNonStreamingPath() throws IOException {
        when(mockSearchContext.isStreamingSearch()).thenReturn(false);
        when(mockSearchContext.getStreamingMode()).thenReturn(null);

        assertFalse(mockSearchContext.isStreamingSearch());
    }
}

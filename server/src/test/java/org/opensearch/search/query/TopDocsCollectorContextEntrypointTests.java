/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.common.util.BigArrays;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests TopDocsCollectorContext routing to streaming collectors.
 */
public class TopDocsCollectorContextEntrypointTests extends OpenSearchTestCase {

    public void testStreamingBranchSelectedWhenStreamingEnabled() throws IOException {
        SearchContext mockSearchContext = mock(SearchContext.class);
        BigArrays mockBigArrays = mock(BigArrays.class);

        when(mockSearchContext.isStreamingSearch()).thenReturn(true);
        when(mockSearchContext.getStreamingMode()).thenReturn(StreamingSearchMode.NO_SCORING);
        when(mockSearchContext.size()).thenReturn(10);
        when(mockSearchContext.bigArrays()).thenReturn(mockBigArrays);

        TopDocsCollectorContext context = TopDocsCollectorContext.createTopDocsCollectorContext(
            mockSearchContext,
            false
        );

        assertNotNull(context);
        assertTrue(context instanceof StreamingUnsortedCollectorContext);
        assertEquals(10, context.numHits());
    }

    public void testStreamingBranchSelectedForScoredSorted() throws IOException {
        SearchContext mockSearchContext = mock(SearchContext.class);
        BigArrays mockBigArrays = mock(BigArrays.class);

        when(mockSearchContext.isStreamingSearch()).thenReturn(true);
        when(mockSearchContext.getStreamingMode()).thenReturn(StreamingSearchMode.SCORED_SORTED);
        when(mockSearchContext.size()).thenReturn(10);
        when(mockSearchContext.bigArrays()).thenReturn(mockBigArrays);
        when(mockSearchContext.sort()).thenReturn(null);

        TopDocsCollectorContext context = TopDocsCollectorContext.createTopDocsCollectorContext(
            mockSearchContext,
            false
        );

        assertNotNull(context);
        assertTrue(context instanceof StreamingSortedCollectorContext);
        assertEquals(10, context.numHits());
    }

    public void testStreamingBranchSelectedForScoredUnsorted() throws IOException {
        SearchContext mockSearchContext = mock(SearchContext.class);
        BigArrays mockBigArrays = mock(BigArrays.class);

        when(mockSearchContext.isStreamingSearch()).thenReturn(true);
        when(mockSearchContext.getStreamingMode()).thenReturn(StreamingSearchMode.SCORED_UNSORTED);
        when(mockSearchContext.size()).thenReturn(10);
        when(mockSearchContext.bigArrays()).thenReturn(mockBigArrays);

        TopDocsCollectorContext context = TopDocsCollectorContext.createTopDocsCollectorContext(
            mockSearchContext,
            false
        );

        assertNotNull(context);
        assertTrue(context instanceof StreamingScoredUnsortedCollectorContext);
        assertEquals(10, context.numHits());
    }

    public void testNonStreamingBranchWhenStreamingDisabled() throws IOException {
        SearchContext mockSearchContext = mock(SearchContext.class);
        BigArrays mockBigArrays = mock(BigArrays.class);

        when(mockSearchContext.isStreamingSearch()).thenReturn(false);
        when(mockSearchContext.getStreamingMode()).thenReturn(StreamingSearchMode.NO_SCORING);

        assertFalse(mockSearchContext.isStreamingSearch());
    }

    public void testNonStreamingBranchWhenModeIsNull() throws IOException {
        SearchContext mockSearchContext = mock(SearchContext.class);
        BigArrays mockBigArrays = mock(BigArrays.class);

        when(mockSearchContext.isStreamingSearch()).thenReturn(true);
        when(mockSearchContext.getStreamingMode()).thenReturn(null);

        assertTrue(mockSearchContext.isStreamingSearch());
        assertNull(mockSearchContext.getStreamingMode());
    }
}

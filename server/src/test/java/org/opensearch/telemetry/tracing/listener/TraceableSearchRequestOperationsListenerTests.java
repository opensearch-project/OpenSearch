/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.listener;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.telemetry.tracing.AttributeNames;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TraceableSearchRequestOperationsListenerTests extends OpenSearchTestCase {

    public void testRequestEndWithoutSearchAddsNoTotalHits() {
        Span requestSpan = mock(Span.class);
        TraceableSearchRequestOperationsListener listener = new TraceableSearchRequestOperationsListener(mock(Tracer.class), requestSpan);

        listener.onRequestEnd(mock(SearchRequestContext.class));

        verifyNoInteractions(requestSpan);
    }

    public void testRequestEndAddsTotalHitsOfItsSearch() {
        Span requestSpan = mock(Span.class);
        TraceableSearchRequestOperationsListener listener = new TraceableSearchRequestOperationsListener(mock(Tracer.class), requestSpan);
        SearchRequestContext searchRequestContext = mock(SearchRequestContext.class);
        when(searchRequestContext.getPhaseContext()).thenReturn(mock(SearchPhaseContext.class));
        when(searchRequestContext.totalHits()).thenReturn(new TotalHits(5, TotalHits.Relation.EQUAL_TO));

        listener.onRequestEnd(searchRequestContext);

        verify(requestSpan).addAttribute(AttributeNames.TOTAL_HITS, 5L);
    }
}

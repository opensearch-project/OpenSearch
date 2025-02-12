/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class DefaultScopedSpanTests extends OpenSearchTestCase {

    @SuppressWarnings("unchecked")
    public void testClose() {
        Span mockSpan = mock(Span.class);
        SpanScope mockSpanScope = mock(SpanScope.class);
        DefaultScopedSpan defaultSpanScope = new DefaultScopedSpan(mockSpan, mockSpanScope);
        defaultSpanScope.close();

        verify(mockSpan).endSpan();
        verify(mockSpanScope).close();
    }

    public void testAddSpanAttributeString() {
        Span mockSpan = mock(Span.class);
        SpanScope mockSpanScope = mock(SpanScope.class);
        DefaultScopedSpan defaultSpanScope = new DefaultScopedSpan(mockSpan, mockSpanScope);
        defaultSpanScope.addAttribute("key", "value");

        verify(mockSpan).addAttribute("key", "value");
    }

    public void testAddSpanAttributeLong() {
        Span mockSpan = mock(Span.class);
        SpanScope mockSpanScope = mock(SpanScope.class);
        DefaultScopedSpan defaultSpanScope = new DefaultScopedSpan(mockSpan, mockSpanScope);
        defaultSpanScope.addAttribute("key", 1L);

        verify(mockSpan).addAttribute("key", 1L);
    }

    public void testAddSpanAttributeDouble() {
        Span mockSpan = mock(Span.class);
        SpanScope mockSpanScope = mock(SpanScope.class);
        DefaultScopedSpan defaultSpanScope = new DefaultScopedSpan(mockSpan, mockSpanScope);
        defaultSpanScope.addAttribute("key", 1.0);

        verify(mockSpan).addAttribute("key", 1.0);
    }

    public void testAddSpanAttributeBoolean() {
        Span mockSpan = mock(Span.class);
        SpanScope mockSpanScope = mock(SpanScope.class);
        DefaultScopedSpan defaultSpanScope = new DefaultScopedSpan(mockSpan, mockSpanScope);
        defaultSpanScope.addAttribute("key", true);

        verify(mockSpan).addAttribute("key", true);
    }

    public void testAddEvent() {
        Span mockSpan = mock(Span.class);
        SpanScope mockSpanScope = mock(SpanScope.class);
        DefaultScopedSpan defaultSpanScope = new DefaultScopedSpan(mockSpan, mockSpanScope);
        defaultSpanScope.addEvent("eventName");

        verify(mockSpan).addEvent("eventName");
    }

    public void testSetError() {
        Span mockSpan = mock(Span.class);
        SpanScope mockSpanScope = mock(SpanScope.class);
        DefaultScopedSpan defaultSpanScope = new DefaultScopedSpan(mockSpan, mockSpanScope);
        Exception ex = new Exception("error");
        defaultSpanScope.setError(ex);

        verify(mockSpan).setError(ex);
    }

}

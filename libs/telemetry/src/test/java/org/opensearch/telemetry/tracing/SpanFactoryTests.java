/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.telemetry.tracing.noop.NoopSpan;
import org.opensearch.test.OpenSearchTestCase;

import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SpanFactoryTests extends OpenSearchTestCase {

    public void testCreateSpanLevelDisabledReturnsNoopSpan() {
        Supplier<Level> levelSupplier = () -> Level.ROOT;
        SpanFactory spanFactory = new SpanFactory(levelSupplier, null);

        assertTrue(spanFactory.createSpan("spanName", null, Level.INFO) instanceof NoopSpan);
    }

    public void testCreateSpanLevelEnabledReturnsDefaultSpan() {
        Supplier<Level> levelSupplier = () -> Level.INFO;
        TracingTelemetry mockTracingTelemetry = mock(TracingTelemetry.class);
        when(mockTracingTelemetry.createSpan(eq("spanName"), any(), eq(Level.INFO))).thenReturn(mock(Span.class));
        SpanFactory spanFactory = new SpanFactory(levelSupplier, mockTracingTelemetry);

        assertFalse(spanFactory.createSpan("spanName", null, Level.INFO) instanceof NoopSpan);
    }

    public void testCreateSpanWithSpanLevelLessThanParentSpanLeve() {
        Supplier<Level> configuredLevelSupplier = () -> Level.INFO;
        TracingTelemetry mockTracingTelemetry = mock(TracingTelemetry.class);
        Span mockParentSpan = mock(Span.class);
        when(mockParentSpan.getLevel()).thenReturn(Level.TERSE);
        when(mockTracingTelemetry.createSpan(eq("spanName"), any(), eq(Level.INFO))).thenReturn(mock(Span.class));
        SpanFactory spanFactory = new SpanFactory(configuredLevelSupplier, mockTracingTelemetry);

        assertFalse(spanFactory.createSpan("spanName", mockParentSpan, Level.INFO) instanceof NoopSpan);
    }

    public void testCreateSpanWithSpanLevelMoreThanParentSpanLeve() {
        Supplier<Level> configuredLevelSupplier = () -> Level.INFO;
        TracingTelemetry mockTracingTelemetry = mock(TracingTelemetry.class);
        Span mockParentSpan = mock(Span.class);
        when(mockParentSpan.getLevel()).thenReturn(Level.DEBUG);
        when(mockTracingTelemetry.createSpan(eq("spanName"), any(), eq(Level.INFO))).thenReturn(mock(Span.class));
        SpanFactory spanFactory = new SpanFactory(configuredLevelSupplier, mockTracingTelemetry);

        assertTrue(spanFactory.createSpan("spanName", mockParentSpan, Level.INFO) instanceof NoopSpan);
    }

    public void testCreateSpanWithSpanLevelSameAsParentSpanLeve() {
        Supplier<Level> configuredLevelSupplier = () -> Level.INFO;
        TracingTelemetry mockTracingTelemetry = mock(TracingTelemetry.class);
        Span mockParentSpan = mock(Span.class);
        when(mockParentSpan.getLevel()).thenReturn(Level.TERSE);
        when(mockTracingTelemetry.createSpan(eq("spanName"), any(), eq(Level.INFO))).thenReturn(mock(Span.class));
        SpanFactory spanFactory = new SpanFactory(configuredLevelSupplier, mockTracingTelemetry);

        assertFalse(spanFactory.createSpan("spanName", mockParentSpan, Level.INFO) instanceof NoopSpan);
    }
}

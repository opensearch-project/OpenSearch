/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch;

import org.junit.Assert;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.telemetry.tracing.DefaultTracer;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.TracerSettings;
import org.opensearch.telemetry.tracing.TracingTelemetry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.telemetry.tracing.DefaultTracer.SPAN_ID;

public class DefaultTracerTests extends OpenSearchTestCase {

    private ThreadPool testThreadPool;
    private TracingTelemetry mockTracingTelemetry;
    private Span mockSpan;
    private Span mockParentSpan;
    private TracerSettings tracerSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        testThreadPool = new TestThreadPool(getTestName());
        setupMocks();
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(testThreadPool, 30, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testCreateSpan() {
        DefaultTracer defaultTracer = new DefaultTracer(mockTracingTelemetry, testThreadPool, tracerSettings);

        defaultTracer.startSpan("span_name", Level.INFO);

        verify(mockSpan).addAttribute(SPAN_ID, "span_id");
        verify(mockSpan).addAttribute(TRACE_ID, "trace_id");
        verify(mockSpan).addAttribute(SPAN_NAME, "span_name");
        Assert.assertEquals("span_name", defaultTracer.getCurrentSpan().getSpanName());
    }

    public void testEndSpan() {
        DefaultTracer defaultTracer = new DefaultTracer(mockTracingTelemetry, testThreadPool, tracerSettings);
        defaultTracer.startSpan("span_name", Level.INFO);

        defaultTracer.endSpan();
        verify(mockSpan).endSpan();
        Assert.assertEquals("parent_span_id", defaultTracer.getCurrentSpan().getSpanId());
    }

    public void testAddSpanAttributeString() {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, testThreadPool, tracerSettings);
        defaultTracer.startSpan("span_name", Level.INFO);

        defaultTracer.addSpanAttribute("key", "value");

        verify(mockSpan).addAttribute("key", "value");
    }

    public void testAddSpanAttributeLong() {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, testThreadPool, tracerSettings);
        defaultTracer.startSpan("span_name", Level.INFO);

        defaultTracer.addSpanAttribute("key", 1L);

        verify(mockSpan).addAttribute("key", 1L);
    }

    public void testAddSpanAttributeDouble() {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, testThreadPool, tracerSettings);
        defaultTracer.startSpan("span_name", Level.INFO);

        defaultTracer.addSpanAttribute("key", 1.0);

        verify(mockSpan).addAttribute("key", 1.0);
    }

    public void testAddSpanAttributeBoolean() {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, testThreadPool, tracerSettings);
        defaultTracer.startSpan("span_name", Level.INFO);

        defaultTracer.addSpanAttribute("key", true);

        verify(mockSpan).addAttribute("key", true);
    }

    public void testAddEvent() {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, testThreadPool, tracerSettings);
        defaultTracer.startSpan("span_name", Level.INFO);

        defaultTracer.addSpanEvent("eventName");

        verify(mockSpan).addEvent("eventName");
    }

    public void testClose() throws IOException {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, testThreadPool, tracerSettings);

        defaultTracer.close();

        verify(mockTracingTelemetry).close();
    }

    private Set<Setting<?>> getClusterSettings() {
        Set<Setting<?>> allTracerSettings = new HashSet<>();
        ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TRACER)).stream().forEach((allTracerSettings::add));
        return allTracerSettings;
    }

    private void setupMocks() {
        Settings settings = Settings.builder().put(TracerSettings.TRACER_LEVEL_SETTING.getKey(), Level.INFO).build();
        tracerSettings = new TracerSettings(settings, new ClusterSettings(settings, getClusterSettings()));
        mockTracingTelemetry = mock(TracingTelemetry.class);
        mockSpan = mock(Span.class);
        mockParentSpan = mock(Span.class);
        when(mockSpan.getSpanName()).thenReturn("span_name");
        when(mockSpan.getSpanId()).thenReturn("span_id");
        when(mockSpan.getTraceId()).thenReturn("trace_id");
        when(mockSpan.getParentSpan()).thenReturn(mockParentSpan);
        when(mockParentSpan.getSpanId()).thenReturn("parent_span_id");
        when(mockParentSpan.getTraceId()).thenReturn("trace_id");
        testThreadPool.getThreadContext().putTransient(CURRENT_SPAN, new AtomicReference<>(mockParentSpan));
        when(mockTracingTelemetry.createSpan("span_name", mockParentSpan, Level.INFO)).thenReturn(mockSpan);
    }
}

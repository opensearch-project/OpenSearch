/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.opensearch.telemetry.tracing.Span;

/**
 * Strict check span processor to validate the spans.
 */
public class StrictCheckSpanProcessor implements SpanProcessor {
    private final Map<String, StackTraceElement[]> spanMap = new ConcurrentHashMap<>();

    /**
     * Base constructor.
     */
    public StrictCheckSpanProcessor() {

    }

    private static Map<String, MockSpanData> finishedSpanItems = new ConcurrentHashMap<>();

    @Override
    public void onStart(Span span) {
        spanMap.put(span.getSpanId(), Thread.currentThread().getStackTrace());
        finishedSpanItems.put(span.getSpanId(), toMockSpanData(span));
    }

    @Override
    public void onEnd(Span span) {
        MockSpanData spanData = finishedSpanItems.get(span.getSpanId());
        if (spanData != null) {
            spanData.setEndEpochNanos(System.nanoTime());
            spanData.setHasEnded(true);
        }
        spanMap.remove(span.getSpanId());
        if (spanMap.containsKey(span.getSpanId())) {
            spanMap.remove(span.getSpanId());
        }
    }

    /**
     * Return list of mock span data at any point of time.
     */
    public List<MockSpanData> getFinishedSpanItems() {
        return new ArrayList<>(finishedSpanItems.values());
    }

    private MockSpanData toMockSpanData(Span span) {
        String parentSpanId = (span.getParentSpan() != null) ? span.getParentSpan().getSpanId() : "";
        MockSpanData spanData = new MockSpanData(
            span.getSpanId(),
            parentSpanId,
            span.getTraceId(),
            System.nanoTime(),
            false,
            span.getSpanName(),
            Thread.currentThread().getStackTrace()
        );
        return spanData;
    }
}

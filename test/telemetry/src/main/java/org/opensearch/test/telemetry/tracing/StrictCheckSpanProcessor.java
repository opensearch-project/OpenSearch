/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import org.opensearch.telemetry.tracing.Span;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Strict check span processor to validate the spans.
 */
public class StrictCheckSpanProcessor implements SpanProcessor {
    /**
     * Base constructor.
     */
    public StrictCheckSpanProcessor() {}

    private static Map<String, MockSpanData> spanMap = new ConcurrentHashMap<>();

    @Override
    public void onStart(Span span) {
        spanMap.put(span.getSpanId(), toMockSpanData(span));
    }

    @Override
    public void onEnd(Span span) {
        MockSpanData spanData = spanMap.get(span.getSpanId());
        // Setting EndEpochTime and HasEnded value to true on completion of span.
        if (spanData != null) {
            spanData.setEndEpochNanos(System.nanoTime());
            spanData.setHasEnded(true);
        }
    }

    /**
     * Return list of mock span data at any point of time.
     */
    public List<MockSpanData> getFinishedSpanItems() {
        return new ArrayList<>(spanMap.values());
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

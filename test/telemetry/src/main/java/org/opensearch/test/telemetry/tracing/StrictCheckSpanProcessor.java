/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import java.util.*;
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
        MockSpanData spanData;
        if (span.getParentSpan() == null){
            spanData = new MockSpanData(span.getSpanId(),
                "0000000000", span.getTraceId(), System.nanoTime(), false, span.getSpanName());
        } else {
            spanData = new MockSpanData(span.getSpanId(),
                span.getParentSpan().getSpanId(), span.getTraceId(), System.nanoTime(), false, span.getSpanName());
        }
        MockSpanData put = finishedSpanItems.put(span.getSpanId(), spanData);
    }

    @Override
    public void onEnd(Span span) {
        spanMap.remove(span.getSpanId());
        MockSpanData spanData = finishedSpanItems.get(span.getSpanId());
        if (spanData != null){
            spanData.setEndEpochNanos(System.nanoTime());
            spanData.setHasEnded(true);
        }
        if(spanMap.containsKey(span.getSpanId())) {
            spanMap.remove(span.getSpanId());
        }
    }

    /**
     * Return list of mock span data at any point of time.
     */
    public List<MockSpanData> getFinishedSpanItems() {
        return new ArrayList<>(finishedSpanItems.values());
    }
}

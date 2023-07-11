/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.opensearch.test.telemetry.tracing.MockSpanData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Writes the span to the list of MockSpanData, everytime a span is emitted.
 */
public class InMemorySpanExporter implements SpanExporter {

    private final AtomicBoolean isShutdown = new AtomicBoolean();

    private  static List<MockSpanData> finishedSpanItems = new ArrayList<>();

    /**
     * No-args constructor
     */
    public InMemorySpanExporter() {}

    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
        if (isShutdown.get()) {
            return CompletableResultCode.ofFailure();
        }
        for (SpanData span : spans) {
            finishedSpanItems.add(convertSpanDataToMockSpanData(span));
        }
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
        return CompletableResultCode.ofSuccess();
    }

    /**
     * Returns All the spans generated at any point of time.
     */
    public List<MockSpanData> getFinishedSpanItems() {
        return finishedSpanItems;
    }

    @Override
    public CompletableResultCode shutdown() {
        assertThat("Duplicate shutdown() calls.", isShutdown.compareAndSet(false, true), equalTo(true));
        return CompletableResultCode.ofSuccess();
    }

    private MockSpanData convertSpanDataToMockSpanData(SpanData spanData) {
        MockSpanData span = new MockSpanData(
            spanData.getSpanId(),
            spanData.getParentSpanId(),
            spanData.getTraceId(),
            spanData.getStartEpochNanos(),
            spanData.getEndEpochNanos(),
            spanData.hasEnded(),
            spanData.getName()
        );
        return span;
    }

    /**
     * Used by SpanExporterFactory to instantiate InMemorySpanExporter
     */
    public static InMemorySpanExporter create() {
        return new InMemorySpanExporter();
    }
}

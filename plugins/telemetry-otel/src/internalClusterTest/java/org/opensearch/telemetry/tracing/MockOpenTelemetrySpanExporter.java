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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.test.telemetry.tracing.MockSpanData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Writes the span to the list of MockSpanData, everytime a span is emitted.
 */
public class MockOpenTelemetrySpanExporter implements SpanExporter {
    private final Logger DEFAULT_LOGGER = LogManager.getLogger(MockOpenTelemetrySpanExporter.class);

    private final AtomicBoolean isShutdown = new AtomicBoolean();

    private List<MockSpanData> finishedSpanItems = new ArrayList<>();

    /**
     * No-args constructor
     */
    public MockOpenTelemetrySpanExporter() {}

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
        if (!isShutdown.compareAndSet(false, true)) {
            DEFAULT_LOGGER.info("Duplicate shutdown() calls.");
        }
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
}

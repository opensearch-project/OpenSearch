/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.test.telemetry.tracing.validators.AllSpansAreEndedProperly;
import org.opensearch.test.telemetry.tracing.validators.AllSpansHaveUniqueId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Strict check span processor to validate the spans.
 */
public class StrictCheckSpanProcessor implements SpanProcessor {

    private final static Logger logger = LogManager.getLogger(StrictCheckSpanProcessor.class);

    /**
     * Base constructor.
     */
    public StrictCheckSpanProcessor() {
        initProcessor();
    }

    private void initProcessor() {
        // restartTheProcessor();
    }

    private static Map<String, MockSpanData> spanMap = new ConcurrentHashMap<>();
    private static final AtomicBoolean shutdown = new AtomicBoolean(false);

    @Override
    public void onStart(Span span) {
        if (shutdown.get() == false) {
            logger.info("Adding span {} {}", Thread.currentThread().getName(), span.getSpanId());
            spanMap.put(span.getSpanId(), toMockSpanData(span));
        }
    }

    @Override
    public void onEnd(Span span) {
        logger.info("End Span");
        MockSpanData spanData = spanMap.get(span.getSpanId());
        // Setting EndEpochTime and HasEnded value to true on completion of span.
        if (spanData != null) {
            logger.info("End Span inside {} {} ", Thread.currentThread().getName(), span.getSpanId());
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
            Thread.currentThread().getStackTrace(),
            (span instanceof MockSpan) ? ((MockSpan) span).getAttributes() : Map.of()
        );
        return spanData;
    }

    /**
     * shutdown
     */
    public static synchronized void shutdown() {
        if (shutdown.get() == false) {
            // shutdown.set(true);
        }
    }

    /**
     * restartTheProcessor
     */
    public static synchronized void restartTheProcessor() {
        if (shutdown.get() == true) {
            shutdown.set(false);
        }
    }

    /**
     * Ensures the strict check succeeds for all the spans.
     */
    public static void validateTracingStateOnShutdown() {
        logger.info("Verifying the tracing at shutdown {}", Thread.currentThread().getName());
        List<MockSpanData> spanData = new ArrayList<>(spanMap.values());
        if (spanData.size() != 0) {
            TelemetryValidators validators = new TelemetryValidators(
                Arrays.asList(new AllSpansAreEndedProperly(), new AllSpansHaveUniqueId())
            );
            try {
                validators.validate(spanData, 1);
            } catch (Error e) {
                spanMap.clear();
                throw e;
            } finally {
                // restartTheProcessor();
            }
        }

    }
}

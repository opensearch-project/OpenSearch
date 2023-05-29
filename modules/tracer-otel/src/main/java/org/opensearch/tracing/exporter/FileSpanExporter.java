/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.exporter;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Writes the span to a log file. This is mostly for testing where you can trace one request and all your spans
 * will be written to a file. Tracing log file would be available in the logs directory.
 */
public class FileSpanExporter implements SpanExporter {
    static final String TRACING_LOG_PREFIX = "tracing_log";
    private static final Logger TRACING_LOGGER = LogManager.getLogger(TRACING_LOG_PREFIX);
    private final Logger DEFAULT_LOGGER = LogManager.getLogger(FileSpanExporter.class);
    private final String FIELD_SEPARATOR = "\t";

    private final AtomicBoolean isShutdown = new AtomicBoolean();

    /**
     * No-args constructor
     */
    public FileSpanExporter() {}

    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
        if (isShutdown.get()) {
            return CompletableResultCode.ofFailure();
        }
        StringBuilder sb = new StringBuilder(128);
        for (SpanData span : spans) {
            sb.setLength(0);
            InstrumentationScopeInfo instrumentationScopeInfo = span.getInstrumentationScopeInfo();
            sb.append("'")
                .append(span.getName())
                .append("'")
                .append(FIELD_SEPARATOR)
                .append(span.getTraceId())
                .append(FIELD_SEPARATOR)
                .append(span.getSpanId())
                .append(FIELD_SEPARATOR)
                .append(span.getParentSpanId())
                .append(FIELD_SEPARATOR)
                .append(span.getKind())
                .append(FIELD_SEPARATOR)
                .append(span.getStartEpochNanos())
                .append(FIELD_SEPARATOR)
                .append(span.getEndEpochNanos())
                .append(FIELD_SEPARATOR)
                .append("[tracer:")
                .append(instrumentationScopeInfo.getName())
                .append(":")
                .append(instrumentationScopeInfo.getVersion() == null ? "" : instrumentationScopeInfo.getVersion())
                .append("]")
                .append(FIELD_SEPARATOR)
                .append(span.getAttributes());
            TRACING_LOGGER.info(sb.toString());
        }
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
        if (!isShutdown.compareAndSet(false, true)) {
            DEFAULT_LOGGER.info("Duplicate shutdown() calls.");
        }
        return CompletableResultCode.ofSuccess();
    }
}

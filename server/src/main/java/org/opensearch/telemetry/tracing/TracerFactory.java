/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.telemetry.Telemetry;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.telemetry.tracing.noop.NoopTracer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/**
 * TracerManager represents a single global class that is used to access tracers.
 * <p>
 * The Tracer singleton object can be retrieved using tracerManager.getTracer(). The TracerManager object
 * is created during class initialization and cannot subsequently be changed.
 */
public class TracerFactory implements Closeable {

    private static final Logger logger = LogManager.getLogger(TracerFactory.class);

    private final TelemetrySettings telemetrySettings;
    private final Tracer tracer;

    public TracerFactory(TelemetrySettings telemetrySettings, Optional<Telemetry> telemetry, ThreadContext threadContext) {
        this.telemetrySettings = telemetrySettings;
        this.tracer = tracer(telemetry, threadContext);
    }

    /**
     * Returns the tracer instance
     *
     * @return tracer instance
     */
    public Tracer getTracer() {
        return tracer;
    }

    /**
     * Closes the {@link Tracer}
     */
    @Override
    public void close() {
        try {
            tracer.close();
        } catch (IOException e) {
            logger.warn("Error closing tracer", e);
        }
    }

    private Tracer tracer(Optional<Telemetry> telemetry, ThreadContext threadContext) {
        return telemetry.map(Telemetry::getTracingTelemetry)
            .map(tracingTelemetry -> createDefaultTracer(tracingTelemetry, threadContext))
            .map(defaultTracer -> createWrappedTracer(defaultTracer))
            .orElse(NoopTracer.INSTANCE);
    }

    private Tracer createDefaultTracer(TracingTelemetry tracingTelemetry, ThreadContext threadContext) {
        TracerContextStorage<String, Span> tracerContextStorage = new ThreadContextBasedTracerContextStorage(
            threadContext,
            tracingTelemetry
        );
        return new DefaultTracer(tracingTelemetry, tracerContextStorage);
    }

    private Tracer createWrappedTracer(Tracer defaultTracer) {
        return new WrappedTracer(telemetrySettings, defaultTracer);
    }

}

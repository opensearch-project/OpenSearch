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
import org.opensearch.telemetry.Telemetry;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;

/**
 * TracerManager represents a single global class that is used to access tracers.
 *
 * The Tracer singleton object can be retrieved using TracerManager.getTracer(). The TracerManager object
 * is created during class initialization and cannot subsequently be changed.
 */
public class TracerFactory implements Closeable {

    private static final Logger logger = LogManager.getLogger(TracerFactory.class);

    private volatile Tracer defaultTracer;
    private final Object mutex = new Object();
    private final TelemetrySettings telemetrySettings;
    private final Telemetry telemetry;
    private final ThreadPool threadPool;

    // TODO Hack, fix me
    public static TracingContextPropagator propagator;

    public TracerFactory(TelemetrySettings telemetrySettings, Telemetry telemetry, ThreadPool threadPool) {
        this.telemetrySettings = telemetrySettings;
        this.telemetry = telemetry;
        this.threadPool = threadPool;
        if (telemetry != null) {
            propagator = telemetry.getTracingTelemetry().getContextPropagator();
        }
    }

    /**
     * Returns the tracer instance
     * @return tracer instance
     */
    public Tracer getTracer() {
        return isTracingDisabled() ? NoopTracer.INSTANCE : getOrCreateDefaultTracerInstance();
    }

    /**
     * Closes the {@link Tracer}
     */
    @Override
    public void close() {
        if (defaultTracer != null) {
            try {
                defaultTracer.close();
            } catch (IOException e) {
                logger.warn("Error closing tracer", e);
            }
        }
    }

    private boolean isTracingDisabled() {
        return !telemetrySettings.isTracingEnabled();
    }

    private Tracer getOrCreateDefaultTracerInstance() {
        if (defaultTracer == null) {
            synchronized (mutex) {
                if (defaultTracer == null) {
                    logger.info("Creating default tracer...");
                    TracingTelemetry tracingTelemetry = telemetry.getTracingTelemetry();
                    TracerContextStorage tracerContextStorage = new ThreadContextBasedTracerContextStorage(
                        threadPool.getThreadContext(),
                        tracingTelemetry
                    );
                    defaultTracer = new DefaultTracer(tracingTelemetry, tracerContextStorage);
                }
            }
        }
        return defaultTracer;
    }

}

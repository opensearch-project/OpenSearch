/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import io.opentelemetry.api.OpenTelemetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * TracerFactory represents a single global class that is used to access tracers.
 *
 * The Tracer singleton object can be retrieved using TracerFactory.getTracer(). The TracerFactory object
 * is created during class initialization and cannot subsequently be changed.
 */
public class TracerFactory {

    private static final Logger logger = LogManager.getLogger(TracerFactory.class);
    private static final Tracer noopTracer = new NoopTracer();
    private static volatile TracerFactory INSTANCE;

    private final ThreadPool threadPool;
    private final TracerSettings tracerSettings;
    private final Object mutex = new Object();
    private volatile Tracer defaultTracer;

    /**
     * Initializes the TracerFactory singleton instance
     * @param threadPool threadpool instance
     * @param tracerSettings tracer settings instance
     */
    public static synchronized void initTracerFactory(ThreadPool threadPool, TracerSettings tracerSettings) {
        if (INSTANCE == null) {
            INSTANCE = new TracerFactory(threadPool, tracerSettings);
        } else {
            logger.warn("Trying to double initialize TracerFactory, skipping");
        }
    }

    /**
     * Returns the {@link Tracer} singleton instance
     * @return Tracer instance
     */
    public static Tracer getTracer() {
        return INSTANCE == null ? noopTracer : INSTANCE.tracer();
    }

    /**
     * Closes the {@link Tracer}
     */
    public static void closeTracer() {
        if (INSTANCE != null && INSTANCE.defaultTracer != null) {
            try {
                INSTANCE.defaultTracer.close();
            } catch (IOException e) {
                logger.warn("Error closing tracer", e);
            }
        }
    }

    public TracerFactory(ThreadPool threadPool, TracerSettings tracerSettings) {
        this.threadPool = threadPool;
        this.tracerSettings = tracerSettings;
    }

    private Tracer tracer() {
        return isTracingDisabled() ? noopTracer : getOrCreateDefaultTracerInstance();
    }

    private boolean isTracingDisabled() {
        return Level.DISABLED == tracerSettings.getTracerLevel();
    }

    private Tracer getOrCreateDefaultTracerInstance() {
        if (defaultTracer == null) {
            synchronized (mutex) {
                if (defaultTracer == null) {
                    logger.info("Creating Otel tracer...");
                    OpenTelemetry openTelemetry = OTelResourceProvider.getOrCreateOpenTelemetryInstance(tracerSettings);
                    defaultTracer = new DefaultTracer(openTelemetry, threadPool, tracerSettings);
                }
            }
        }
        return defaultTracer;
    }

    // for testing
    static void clear() {
        INSTANCE = null;
    }

}

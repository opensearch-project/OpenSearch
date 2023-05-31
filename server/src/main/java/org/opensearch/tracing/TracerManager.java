/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.tracing.noop.NoopTracer;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * TracerManager represents a single global class that is used to access tracers.
 *
 * The Tracer singleton object can be retrieved using TracerManager.getTracer(). The TracerManager object
 * is created during class initialization and cannot subsequently be changed.
 */
public class TracerManager {

    private static final Logger logger = LogManager.getLogger(TracerManager.class);
    private static volatile TracerManager INSTANCE;

    private volatile Tracer defaultTracer;
    private final Object mutex = new Object();
    private final TracerSettings tracerSettings;
    private final Supplier<Telemetry> telemetrySupplier;
    private final ThreadPool threadPool;

    /**
     * Initializes the TracerFactory singleton instance
     *
     * @param tracerSettings       tracer settings instance
     * @param threadPool
     */
    public static synchronized void initTracerManager(
        TracerSettings tracerSettings,
        Supplier<Telemetry> tracerSupplier,
        ThreadPool threadPool) {
        if (INSTANCE == null) {
            INSTANCE = new TracerManager(tracerSettings, tracerSupplier, threadPool);
        } else {
            logger.warn("Trying to double initialize TracerFactory, skipping");
        }
    }

    /**
     * Returns the {@link Tracer} singleton instance
     * @return Tracer instance
     */
    public static Tracer getTracer() {
        return INSTANCE == null ? NoopTracer.INSTANCE : INSTANCE.tracer();
    }

    public static BiConsumer<Map<String, String>, Map<String, Object>> getTracerHeaderInjector() {
        return INSTANCE == null ? (x,y) -> {} : INSTANCE.tracerHeaderInjector();
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

    public TracerManager(TracerSettings tracerSettings, Supplier<Telemetry> telemetrySupplier, ThreadPool threadPool) {
        this.tracerSettings = tracerSettings;
        this.telemetrySupplier = telemetrySupplier;
        this.threadPool = threadPool;
    }

    private Tracer tracer() {
        return isTracingDisabled() ? NoopTracer.INSTANCE : getOrCreateDefaultTracerInstance();
    }

    private BiConsumer<Map<String, String>, Map<String, Object>> tracerHeaderInjector() {
        return isTracingDisabled() ? (x,y) -> {} : telemetrySupplier.get().injectSpanInHeader();
    }

    private boolean isTracingDisabled() {
        return Level.DISABLED == tracerSettings.getTracerLevel();
    }

    private Tracer getOrCreateDefaultTracerInstance() {
        if (defaultTracer == null) {
            synchronized (mutex) {
                if (defaultTracer == null) {
                    logger.info("Creating Otel tracer...");
                    defaultTracer = new DefaultTracer(telemetrySupplier.get(), threadPool, tracerSettings);
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

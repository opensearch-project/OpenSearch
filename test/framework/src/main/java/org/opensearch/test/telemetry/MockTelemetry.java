/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry;

import org.opensearch.telemetry.Telemetry;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.telemetry.metrics.MetricsTelemetry;
import org.opensearch.telemetry.tracing.TracingTelemetry;
import org.opensearch.test.telemetry.tracing.MockTracingTelemetry;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;

/**
 * Mock {@link Telemetry} implementation for testing.
 */
public class MockTelemetry implements Telemetry {
    private final ThreadPool threadPool;

    /**
     * Constructor with settings.
     * @param settings telemetry settings.
     */
    public MockTelemetry(TelemetrySettings settings) {
        this(settings, null);
    }

    /**
     * Constructor with settings.
     * @param settings telemetry settings.
     * @param threadPool thread pool to watch for termination
     */
    public MockTelemetry(TelemetrySettings settings, ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public TracingTelemetry getTracingTelemetry() {
        return new MockTracingTelemetry(() -> {
            // There could be some asynchronous tasks running that we should await for before the closing
            // up the tracer instance.
            if (threadPool != null) {
                try {
                    threadPool.awaitTermination(10, TimeUnit.SECONDS);
                } catch (final InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    @Override
    public MetricsTelemetry getMetricsTelemetry() {
        return new MetricsTelemetry() {
        };
    }
}

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
import org.opensearch.test.telemetry.tracing.MockTracingTelemetry;
import org.opensearch.telemetry.tracing.TracingTelemetry;

/**
 * Mock {@link Telemetry} implementation for testing.
 */
public class MockTelemetry implements Telemetry {

    private final TelemetrySettings settings;

    /**
     * Constructor with settings.
     * @param settings telemetry settings.
     */
    public MockTelemetry(TelemetrySettings settings) {
        this.settings = settings;
    }

    @Override
    public TracingTelemetry getTracingTelemetry() {
        return new MockTracingTelemetry();
    }

    @Override
    public MetricsTelemetry getMetricsTelemetry() {
        return new MetricsTelemetry() {
        };
    }
}

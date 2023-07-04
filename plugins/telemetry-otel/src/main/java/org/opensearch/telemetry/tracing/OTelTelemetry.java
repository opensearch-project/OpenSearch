/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.telemetry.Telemetry;
import org.opensearch.telemetry.metrics.MetricsTelemetry;

/**
 * Otel implementation of Telemetry
 */
public class OTelTelemetry implements Telemetry {

    private final TracingTelemetry tracingTelemetry;
    private final MetricsTelemetry metricsTelemetry;

    /**
     * Creates Telemetry instance
     * @param tracingTelemetry tracing telemetry
     * @param metricsTelemetry metrics telemetry
     */
    public OTelTelemetry(TracingTelemetry tracingTelemetry, MetricsTelemetry metricsTelemetry) {
        this.tracingTelemetry = tracingTelemetry;
        this.metricsTelemetry = metricsTelemetry;
    }

    @Override
    public TracingTelemetry getTracingTelemetry() {
        return tracingTelemetry;
    }

    @Override
    public MetricsTelemetry getMetricsTelemetry() {
        return metricsTelemetry;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.concurrent.RefCountedReleasable;
import org.opensearch.telemetry.Telemetry;
import org.opensearch.telemetry.metrics.MetricsTelemetry;
import org.opensearch.telemetry.metrics.OTelMetricsTelemetry;

import io.opentelemetry.sdk.OpenTelemetrySdk;

/**
 * Otel implementation of Telemetry
 */
public class OTelTelemetry implements Telemetry {

    private final RefCountedReleasable<OpenTelemetrySdk> refCountedOpenTelemetry;

    /**
     * Creates Telemetry instance

     */
    /**
     * Creates Telemetry instance
     * @param refCountedOpenTelemetry open telemetry.
     */
    public OTelTelemetry(RefCountedReleasable<OpenTelemetrySdk> refCountedOpenTelemetry) {
        this.refCountedOpenTelemetry = refCountedOpenTelemetry;
    }

    @Override
    public TracingTelemetry getTracingTelemetry() {
        refCountedOpenTelemetry.incRef();
        return new OTelTracingTelemetry<>(refCountedOpenTelemetry, refCountedOpenTelemetry.get().getSdkTracerProvider());
    }

    @Override
    public MetricsTelemetry getMetricsTelemetry() {
        refCountedOpenTelemetry.incRef();
        return new OTelMetricsTelemetry<>(refCountedOpenTelemetry, refCountedOpenTelemetry.get().getSdkMeterProvider());
    }
}

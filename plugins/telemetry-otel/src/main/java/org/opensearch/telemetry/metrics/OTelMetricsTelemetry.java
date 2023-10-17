/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import org.opensearch.common.concurrent.RefCountedReleasable;
import org.opensearch.telemetry.OTelTelemetryPlugin;

import java.io.Closeable;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;

import io.opentelemetry.api.metrics.DoubleCounter;
import io.opentelemetry.api.metrics.DoubleUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.sdk.OpenTelemetrySdk;

/**
 * OTel implementation for {@link MetricsTelemetry}
 */
public class OTelMetricsTelemetry<T extends MeterProvider & Closeable> implements MetricsTelemetry {
    private final RefCountedReleasable<OpenTelemetrySdk> refCountedOpenTelemetry;
    private final Meter otelMeter;
    private final T meterProvider;

    /**
     * Creates OTel based {@link MetricsTelemetry}.
     * @param openTelemetry open telemetry.
     * @param meterProvider {@link MeterProvider} instance
     */
    public OTelMetricsTelemetry(RefCountedReleasable<OpenTelemetrySdk> openTelemetry, T meterProvider) {
        this.refCountedOpenTelemetry = openTelemetry;
        this.meterProvider = meterProvider;
        this.otelMeter = meterProvider.get(OTelTelemetryPlugin.INSTRUMENTATION_SCOPE_NAME);
    }

    @Override
    public Counter createCounter(String name, String description, String unit) {
        DoubleCounter doubleCounter = AccessController.doPrivileged(
            (PrivilegedAction<DoubleCounter>) () -> otelMeter.counterBuilder(name)
                .setUnit(unit)
                .setDescription(description)
                .ofDoubles()
                .build()
        );
        return new OTelCounter(doubleCounter);
    }

    @Override
    public Counter createUpDownCounter(String name, String description, String unit) {
        DoubleUpDownCounter doubleUpDownCounter = AccessController.doPrivileged(
            (PrivilegedAction<DoubleUpDownCounter>) () -> otelMeter.upDownCounterBuilder(name)
                .setUnit(unit)
                .setDescription(description)
                .ofDoubles()
                .build()
        );
        return new OTelUpDownCounter(doubleUpDownCounter);
    }

    @Override
    public void close() throws IOException {
        refCountedOpenTelemetry.close();
    }
}

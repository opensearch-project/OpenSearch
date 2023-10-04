/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.telemetry.OTelTelemetryPlugin;

import java.io.Closeable;
import java.security.AccessController;
import java.security.PrivilegedAction;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.DoubleCounter;
import io.opentelemetry.api.metrics.DoubleUpDownCounter;
import io.opentelemetry.api.metrics.Meter;

/**
 * OTel implementation for {@link MetricsTelemetry}
 */
public class OTelMetricsTelemetry implements MetricsTelemetry {
    private static final Logger logger = LogManager.getLogger(OTelMetricsTelemetry.class);
    private final OpenTelemetry openTelemetry;
    private final Meter otelMeter;
    private final Closeable metricsProviderClosable;

    /**
     * Creates OTel based {@link MetricsTelemetry}.
     * @param openTelemetry telemetry.
     */

    /**
     * Creates OTel based {@link MetricsTelemetry}.
     * @param openTelemetry OpenTelemetry instance
     * @param meterProviderCloseable closable to close the meter.
     */
    public OTelMetricsTelemetry(OpenTelemetry openTelemetry, Closeable meterProviderCloseable) {
        this.openTelemetry = openTelemetry;
        this.otelMeter = openTelemetry.getMeter(OTelTelemetryPlugin.INSTRUMENTATION_SCOPE_NAME);
        this.metricsProviderClosable = meterProviderCloseable;
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
    public void close() {
        try {
            metricsProviderClosable.close();
        } catch (Exception e) {
            logger.warn("Error while closing Opentelemetry MeterProvider", e);
        }
    }
}

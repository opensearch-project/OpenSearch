/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import org.opensearch.telemetry.metrics.tags.Tags;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Default implementation for {@link MetricsRegistry}
 */
class DefaultMetricsRegistry implements MetricsRegistry {
    private final MetricsTelemetry metricsTelemetry;

    /**
     * Constructor
     * @param metricsTelemetry metrics telemetry.
     */
    public DefaultMetricsRegistry(MetricsTelemetry metricsTelemetry) {
        this.metricsTelemetry = metricsTelemetry;
    }

    @Override
    public Counter createCounter(String name, String description, String unit) {
        return metricsTelemetry.createCounter(name, description, unit);
    }

    @Override
    public Counter createUpDownCounter(String name, String description, String unit) {
        return metricsTelemetry.createUpDownCounter(name, description, unit);
    }

    @Override
    public Histogram createHistogram(String name, String description, String unit) {
        return metricsTelemetry.createHistogram(name, description, unit);
    }

    @Override
    public Closeable createGauge(String name, String description, String unit, Supplier<Double> valueProvider, Tags tags) {
        return metricsTelemetry.createGauge(name, description, unit, valueProvider, tags);
    }

    @Override
    public Closeable createGauge(String name, String description, String unit, Supplier<TaggedMeasurement> value) {
        return metricsTelemetry.createGauge(name, description, unit, value);
    }

    @Override
    public void close() throws IOException {
        metricsTelemetry.close();
    }
}

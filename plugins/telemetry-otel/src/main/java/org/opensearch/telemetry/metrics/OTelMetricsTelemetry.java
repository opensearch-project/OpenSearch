/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import java.util.List;
import org.opensearch.common.concurrent.RefCountedReleasable;
import org.opensearch.telemetry.OTelTelemetryPlugin;

import java.io.Closeable;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;

import io.opentelemetry.api.metrics.DoubleCounter;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.DoubleUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import org.opensearch.telemetry.tracing.OTelResourceProvider;

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
        this.refCountedOpenTelemetry.incRef();
        this.meterProvider = meterProvider;
        this.otelMeter = meterProvider.get(OTelTelemetryPlugin.INSTRUMENTATION_SCOPE_NAME);
    }

    @SuppressWarnings("removal")
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

    @SuppressWarnings("removal")
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

    /**
     * Creates the Otel Histogram. It created the default version. In {@link org.opensearch.telemetry.tracing.OTelResourceProvider}
     * we can configure the bucketing strategy through view. It appends the OTelResourceProvider.DYNAMIC_HISTOGRAM_METRIC_NAME_SUFFIX
     * to the metric name. It's needed to differentiate the {@link Histogram} metrics with explictt bucket or dynamic
     * buckets. This suffix can be removed from the metric name in the collector.
     * @param name        name of the histogram.
     * @param description any description about the metric.
     * @param unit        unit of the metric.
     * @return histogram
     */
    @Override
    public Histogram createHistogram(String name, String description, String unit) {
        String internalMetricName = name + OTelResourceProvider.DYNAMIC_HISTOGRAM_METRIC_NAME_SUFFIX;
        DoubleHistogram doubleHistogram = AccessController.doPrivileged(
            (PrivilegedAction<DoubleHistogram>) () -> otelMeter.histogramBuilder(internalMetricName).setUnit(unit).setDescription(description)
                .build()
        );
        return new OTelHistogram(doubleHistogram);
    }

    @Override
    public Histogram createHistogram(String name, String description, String unit, List<Double> buckets) {
        DoubleHistogram doubleHistogram = AccessController.doPrivileged(
            (PrivilegedAction<DoubleHistogram>) () -> otelMeter.histogramBuilder(name).setUnit(unit).setDescription(description)
                .setExplicitBucketBoundariesAdvice(buckets).build()
        );
        return new OTelHistogram(doubleHistogram);
    }

    @Override
    public void close() throws IOException {
        meterProvider.close();
        refCountedOpenTelemetry.close();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import org.opensearch.common.concurrent.RefCountedReleasable;
import org.opensearch.telemetry.OTelAttributesConverter;
import org.opensearch.telemetry.OTelTelemetryPlugin;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.io.Closeable;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.function.Supplier;

import io.opentelemetry.api.metrics.DoubleCounter;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.DoubleUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
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
     * Creates the Otel Histogram. In {@link org.opensearch.telemetry.tracing.OTelResourceProvider}
     * we can configure the bucketing/aggregation strategy through view. Default startegy configured
     * is the {@link io.opentelemetry.sdk.metrics.internal.view.Base2ExponentialHistogramAggregation}.
     * @param name        name of the histogram.
     * @param description any description about the metric.
     * @param unit        unit of the metric.
     * @return histogram
     */
    @Override
    public Histogram createHistogram(String name, String description, String unit) {
        DoubleHistogram doubleHistogram = AccessController.doPrivileged(
            (PrivilegedAction<DoubleHistogram>) () -> otelMeter.histogramBuilder(name).setUnit(unit).setDescription(description).build()
        );
        return new OTelHistogram(doubleHistogram);
    }

    @Override
    public Closeable createGauge(String name, String description, String unit, Supplier<Double> valueProvider, Tags tags) {
        ObservableDoubleGauge doubleObservableGauge = AccessController.doPrivileged(
            (PrivilegedAction<ObservableDoubleGauge>) () -> otelMeter.gaugeBuilder(name)
                .setUnit(unit)
                .setDescription(description)
                .buildWithCallback(record -> record.record(valueProvider.get(), OTelAttributesConverter.convert(tags)))
        );
        return () -> doubleObservableGauge.close();
    }

    @Override
    public Closeable createGauge(String name, String description, String unit, Supplier<TaggedMeasurement> value) {
        ObservableDoubleGauge doubleObservableGauge = AccessController.doPrivileged(
            (PrivilegedAction<ObservableDoubleGauge>) () -> otelMeter.gaugeBuilder(name)
                .setUnit(unit)
                .setDescription(description)
                .buildWithCallback(record -> record.record(value.get().getValue(), OTelAttributesConverter.convert(value.get().getTags())))
        );
        return () -> doubleObservableGauge.close();
    }

    @Override
    public void close() throws IOException {
        meterProvider.close();
        refCountedOpenTelemetry.close();
    }
}

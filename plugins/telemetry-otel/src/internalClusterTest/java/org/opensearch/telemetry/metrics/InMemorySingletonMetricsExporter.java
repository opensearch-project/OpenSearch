/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import java.util.Collection;
import java.util.List;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter;

public class InMemorySingletonMetricsExporter implements MetricExporter {

    public static final InMemorySingletonMetricsExporter INSTANCE = new InMemorySingletonMetricsExporter(InMemoryMetricExporter.create());

    private static InMemoryMetricExporter delegate;

    public static InMemorySingletonMetricsExporter create() {
        return INSTANCE;
    }

    private InMemorySingletonMetricsExporter(InMemoryMetricExporter delegate) {
        InMemorySingletonMetricsExporter.delegate = delegate;
    }

    @Override
    public CompletableResultCode export(Collection<MetricData> metrics) {
        return delegate.export(metrics);
    }

    @Override
    public CompletableResultCode flush() {
        return delegate.flush();
    }

    @Override
    public CompletableResultCode shutdown() {
        return delegate.shutdown();
    }

    public List<MetricData> getFinishedMetricItems() {
        return delegate.getFinishedMetricItems();
    }

    /**
     * Clears the state.
     */
    public void reset() {
        delegate.reset();
    }

    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
        return delegate.getAggregationTemporality(instrumentType);
    }
}

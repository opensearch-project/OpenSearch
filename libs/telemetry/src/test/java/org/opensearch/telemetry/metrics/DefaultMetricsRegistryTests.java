/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;

import java.io.Closeable;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultMetricsRegistryTests extends OpenSearchTestCase {

    private MetricsTelemetry metricsTelemetry;
    private DefaultMetricsRegistry defaultMeterRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        metricsTelemetry = mock(MetricsTelemetry.class);
        defaultMeterRegistry = new DefaultMetricsRegistry(metricsTelemetry);
    }

    public void testCounter() {
        Counter mockCounter = mock(Counter.class);
        when(defaultMeterRegistry.createCounter(any(String.class), any(String.class), any(String.class))).thenReturn(mockCounter);
        Counter counter = defaultMeterRegistry.createCounter(
            "org.opensearch.telemetry.metrics.DefaultMeterRegistryTests.testCounter",
            "test counter",
            "1"
        );
        assertSame(mockCounter, counter);
    }

    public void testUpDownCounter() {
        Counter mockCounter = mock(Counter.class);
        when(defaultMeterRegistry.createUpDownCounter(any(String.class), any(String.class), any(String.class))).thenReturn(mockCounter);
        Counter counter = defaultMeterRegistry.createUpDownCounter(
            "org.opensearch.telemetry.metrics.DefaultMeterRegistryTests.testUpDownCounter",
            "test up-down counter",
            "1"
        );
        assertSame(mockCounter, counter);
    }

    public void testHistogram() {
        Histogram mockHistogram = mock(Histogram.class);
        when(defaultMeterRegistry.createHistogram(any(String.class), any(String.class), any(String.class))).thenReturn(mockHistogram);
        Histogram histogram = defaultMeterRegistry.createHistogram(
            "org.opensearch.telemetry.metrics.DefaultMeterRegistryTests.testHistogram",
            "test up-down counter",
            "ms"
        );
        assertSame(mockHistogram, histogram);
    }

    @SuppressWarnings("unchecked")
    public void testGauge() {
        Closeable mockCloseable = mock(Closeable.class);
        when(
            defaultMeterRegistry.createGauge(any(String.class), any(String.class), any(String.class), any(Supplier.class), any(Tags.class))
        ).thenReturn(mockCloseable);
        Closeable closeable = defaultMeterRegistry.createGauge(
            "org.opensearch.telemetry.metrics.DefaultMeterRegistryTests.testObservableGauge",
            "test observable gauge",
            "ms",
            () -> 1.0,
            Tags.EMPTY
        );
        assertSame(mockCloseable, closeable);
    }

    @SuppressWarnings("unchecked")
    public void testGaugeWithValueAndTagSupplier() {
        Closeable mockCloseable = mock(Closeable.class);
        when(defaultMeterRegistry.createGauge(any(String.class), any(String.class), any(String.class), any(Supplier.class))).thenReturn(
            mockCloseable
        );
        Closeable closeable = defaultMeterRegistry.createGauge(
            "org.opensearch.telemetry.metrics.DefaultMeterRegistryTests.testObservableGauge",
            "test observable gauge",
            "ms",
            () -> TaggedMeasurement.create(1.0, Tags.EMPTY)
        );
        assertSame(mockCloseable, closeable);
    }

}

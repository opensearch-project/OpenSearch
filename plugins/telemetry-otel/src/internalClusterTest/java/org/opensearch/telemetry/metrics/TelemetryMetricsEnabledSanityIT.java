/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.telemetry.IntegrationTestOTelTelemetryPlugin;
import org.opensearch.telemetry.OTelTelemetrySettings;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableExponentialHistogramPointData;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, minNumDataNodes = 1)
public class TelemetryMetricsEnabledSanityIT extends OpenSearchIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(TelemetrySettings.METRICS_FEATURE_ENABLED_SETTING.getKey(), true)
            .put(
                OTelTelemetrySettings.OTEL_METRICS_EXPORTER_CLASS_SETTING.getKey(),
                "org.opensearch.telemetry.metrics.InMemorySingletonMetricsExporter"
            )
            .put(TelemetrySettings.METRICS_PUBLISH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IntegrationTestOTelTelemetryPlugin.class);
    }

    @Override
    protected boolean addMockTelemetryPlugin() {
        return false;
    }

    public void testCounter() throws Exception {
        MetricsRegistry metricsRegistry = internalCluster().getInstance(MetricsRegistry.class);
        InMemorySingletonMetricsExporter.INSTANCE.reset();

        Counter counter = metricsRegistry.createCounter("test-counter", "test", "1");
        counter.add(1.0);
        // Sleep for about 2s to wait for metrics to be published.
        Thread.sleep(2000);

        InMemorySingletonMetricsExporter exporter = InMemorySingletonMetricsExporter.INSTANCE;
        double value = ((DoublePointData) ((ArrayList) exporter.getFinishedMetricItems()
            .stream()
            .filter(a -> a.getName().equals("test-counter"))
            .collect(Collectors.toList())
            .get(0)
            .getDoubleSumData()
            .getPoints()).get(0)).getValue();
        assertEquals(1.0, value, 0.0);
    }

    public void testUpDownCounter() throws Exception {

        MetricsRegistry metricsRegistry = internalCluster().getInstance(MetricsRegistry.class);
        InMemorySingletonMetricsExporter.INSTANCE.reset();

        Counter counter = metricsRegistry.createUpDownCounter("test-up-down-counter", "test", "1");
        counter.add(1.0);
        counter.add(-2.0);
        // Sleep for about 2s to wait for metrics to be published.
        Thread.sleep(2000);

        InMemorySingletonMetricsExporter exporter = InMemorySingletonMetricsExporter.INSTANCE;
        double value = ((DoublePointData) ((ArrayList) exporter.getFinishedMetricItems()
            .stream()
            .filter(a -> a.getName().equals("test-up-down-counter"))
            .collect(Collectors.toList())
            .get(0)
            .getDoubleSumData()
            .getPoints()).get(0)).getValue();
        assertEquals(-1.0, value, 0.0);
    }

    public void testHistogram() throws Exception {
        MetricsRegistry metricsRegistry = internalCluster().getInstance(MetricsRegistry.class);
        InMemorySingletonMetricsExporter.INSTANCE.reset();

        Histogram histogram = metricsRegistry.createHistogram("test-histogram", "test", "ms");
        histogram.record(2.0);
        histogram.record(1.0);
        histogram.record(3.0);
        // Sleep for about 2s to wait for metrics to be published.
        Thread.sleep(2000);

        InMemorySingletonMetricsExporter exporter = InMemorySingletonMetricsExporter.INSTANCE;
        ImmutableExponentialHistogramPointData histogramPointData = ((ImmutableExponentialHistogramPointData) ((ArrayList) exporter
            .getFinishedMetricItems()
            .stream()
            .filter(a -> a.getName().contains("test-histogram"))
            .collect(Collectors.toList())
            .get(0)
            .getExponentialHistogramData()
            .getPoints()).get(0));
        assertEquals(1.0, histogramPointData.getSum(), 6.0);
        assertEquals(1.0, histogramPointData.getMax(), 3.0);
        assertEquals(1.0, histogramPointData.getMin(), 1.0);
    }

    public void testGauge() throws Exception {
        String metricName = "test-gauge";
        MetricsRegistry metricsRegistry = internalCluster().getInstance(MetricsRegistry.class);
        InMemorySingletonMetricsExporter.INSTANCE.reset();
        Tags tags = Tags.create().addTag("test", "integ-test");
        final AtomicInteger testValue = new AtomicInteger(0);
        Supplier<Double> valueProvider = () -> { return Double.valueOf(testValue.incrementAndGet()); };
        Closeable gaugeCloseable = metricsRegistry.createGauge(metricName, "test", "ms", valueProvider, tags);
        // Sleep for about 2.2s to wait for metrics to be published.
        Thread.sleep(2200);

        InMemorySingletonMetricsExporter exporter = InMemorySingletonMetricsExporter.INSTANCE;

        assertTrue(getMaxObservableGaugeValue(exporter, metricName) >= 2.0);
        gaugeCloseable.close();
        double observableGaugeValueAfterStop = getMaxObservableGaugeValue(exporter, metricName);

        // Sleep for about 1.2s to wait for metrics to see that closed observableGauge shouldn't execute the callable.
        Thread.sleep(1200);
        assertEquals(observableGaugeValueAfterStop, getMaxObservableGaugeValue(exporter, metricName), 0.0);

    }

    public void testGaugeWithValueAndTagSupplier() throws Exception {
        String metricName = "test-gauge";
        MetricsRegistry metricsRegistry = internalCluster().getInstance(MetricsRegistry.class);
        InMemorySingletonMetricsExporter.INSTANCE.reset();
        Tags tags = Tags.create().addTag("test", "integ-test");
        final AtomicInteger testValue = new AtomicInteger(0);
        Supplier<TaggedMeasurement> valueProvider = () -> {
            return TaggedMeasurement.create(Double.valueOf(testValue.incrementAndGet()), tags);
        };
        Closeable gaugeCloseable = metricsRegistry.createGauge(metricName, "test", "ms", valueProvider);
        // Sleep for about 2.2s to wait for metrics to be published.
        Thread.sleep(2200);

        InMemorySingletonMetricsExporter exporter = InMemorySingletonMetricsExporter.INSTANCE;

        assertTrue(getMaxObservableGaugeValue(exporter, metricName) >= 2.0);

        gaugeCloseable.close();
        double observableGaugeValueAfterStop = getMaxObservableGaugeValue(exporter, metricName);

        Map<AttributeKey<?>, Object> attributes = getMetricAttributes(exporter, metricName);

        assertEquals("integ-test", attributes.get(AttributeKey.stringKey("test")));

        // Sleep for about 1.2s to wait for metrics to see that closed observableGauge shouldn't execute the callable.
        Thread.sleep(1200);
        assertEquals(observableGaugeValueAfterStop, getMaxObservableGaugeValue(exporter, metricName), 0.0);

    }

    private static double getMaxObservableGaugeValue(InMemorySingletonMetricsExporter exporter, String metricName) {
        List<MetricData> dataPoints = exporter.getFinishedMetricItems()
            .stream()
            .filter(a -> a.getName().contains(metricName))
            .collect(Collectors.toList());
        double totalValue = 0;
        for (MetricData metricData : dataPoints) {
            totalValue = Math.max(totalValue, ((DoublePointData) metricData.getDoubleGaugeData().getPoints().toArray()[0]).getValue());
        }
        return totalValue;
    }

    private static Map<AttributeKey<?>, Object> getMetricAttributes(InMemorySingletonMetricsExporter exporter, String metricName) {
        List<MetricData> dataPoints = exporter.getFinishedMetricItems()
            .stream()
            .filter(a -> a.getName().contains(metricName))
            .collect(Collectors.toList());
        Attributes attributes = dataPoints.get(0).getDoubleGaugeData().getPoints().stream().findAny().get().getAttributes();
        return attributes.asMap();
    }

    @After
    public void reset() {
        InMemorySingletonMetricsExporter.INSTANCE.reset();
    }
}

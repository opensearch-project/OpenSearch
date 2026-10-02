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
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, minNumDataNodes = 1)
public class TelemetryMetricsExplicitHistogramIT extends OpenSearchIntegTestCase {

    // Default explicit bucket boundaries of the OpenTelemetry SDK.
    private static final List<Double> SDK_DEFAULT_BOUNDARIES = Arrays.asList(
        0.0,
        5.0,
        10.0,
        25.0,
        50.0,
        75.0,
        100.0,
        250.0,
        500.0,
        750.0,
        1000.0,
        2500.0,
        5000.0,
        7500.0,
        10000.0
    );

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(TelemetrySettings.METRICS_FEATURE_ENABLED_SETTING.getKey(), true)
            .put(
                OTelTelemetrySettings.OTEL_METRICS_EXPORTER_CLASS_SETTING.getKey(),
                "org.opensearch.telemetry.metrics.InMemorySingletonMetricsExporter"
            )
            .put(OTelTelemetrySettings.OTEL_METRICS_HISTOGRAM_AGGREGATION_SETTING.getKey(), "explicit_bucket_histogram")
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

    public void testHistogramUsesExplicitBuckets() throws Exception {
        MetricsRegistry metricsRegistry = internalCluster().getInstance(MetricsRegistry.class);
        InMemorySingletonMetricsExporter.INSTANCE.reset();

        Histogram histogram = metricsRegistry.createHistogram("test-explicit-histogram", "test", "ms");
        histogram.record(2.0);
        histogram.record(1.0);
        histogram.record(3.0);

        assertBusy(() -> {
            List<MetricData> metrics = InMemorySingletonMetricsExporter.INSTANCE.getFinishedMetricItems()
                .stream()
                .filter(a -> a.getName().contains("test-explicit-histogram"))
                .collect(Collectors.toList());
            assertFalse(metrics.isEmpty());
            assertTrue(metrics.stream().noneMatch(m -> m.getType() == MetricDataType.EXPONENTIAL_HISTOGRAM));

            // Exports are cumulative: pick the export that already contains all three records.
            HistogramPointData point = metrics.stream()
                .filter(m -> m.getType() == MetricDataType.HISTOGRAM)
                .map(m -> m.getHistogramData().getPoints().iterator().next())
                .filter(pt -> pt.getCount() == 3)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no export with all records yet"));
            assertEquals(6.0, point.getSum(), 0.0);
            assertEquals(1.0, point.getMin(), 0.0);
            assertEquals(3.0, point.getMax(), 0.0);
            assertEquals(SDK_DEFAULT_BOUNDARIES, point.getBoundaries());
        });
    }
}

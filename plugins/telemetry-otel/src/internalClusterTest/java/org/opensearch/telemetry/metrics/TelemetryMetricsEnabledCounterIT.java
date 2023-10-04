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
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import io.opentelemetry.sdk.metrics.data.DoublePointData;

public class TelemetryMetricsEnabledCounterIT extends OpenSearchSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(TelemetrySettings.METRICS_FEATURE_ENABLED_SETTING.getKey(), true)
            .put(
                OTelTelemetrySettings.OTEL_METRICS_EXPORTER_CLASS_SETTING.getKey(),
                "org.opensearch.telemetry.metrics.InMemorySingletonMetricsExporter"
            )
            .put(TelemetrySettings.METRICS_PUBLISH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(IntegrationTestOTelTelemetryPlugin.class);
    }

    @Override
    protected boolean addMockTelemetryPlugin() {
        return false;
    }

    public void testCounter() throws Exception {
        MetricsRegistry metricsRegistry = node().injector().getInstance(MetricsRegistry.class);

        Counter counter = metricsRegistry.createCounter("test-counter", "test", "1");
        counter.add(1.0);
        // Sleep for about 2s to wait for metrics to be published.
        Thread.sleep(2000);

        InMemorySingletonMetricsExporter exporter = InMemorySingletonMetricsExporter.INSTANCE;
        double value = ((DoublePointData) ((ArrayList) exporter.getFinishedMetricItems().get(0).getDoubleSumData().getPoints()).get(0))
            .getValue();
        assertEquals(1.0, value, 0.0);
    }
}

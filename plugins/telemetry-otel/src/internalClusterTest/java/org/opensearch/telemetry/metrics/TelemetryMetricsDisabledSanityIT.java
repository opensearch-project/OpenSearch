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
import org.opensearch.telemetry.metrics.noop.NoopCounter;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Arrays;
import java.util.Collection;

public class TelemetryMetricsDisabledSanityIT extends OpenSearchSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(TelemetrySettings.METRICS_FEATURE_ENABLED_SETTING.getKey(), false)
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

    public void testSanityChecksWhenMetricsEnabled() throws Exception {
        MetricsRegistry metricsRegistry = node().injector().getInstance(MetricsRegistry.class);

        Counter counter = metricsRegistry.createCounter("test-counter", "test", "1");
        counter.add(1.0);

        assertTrue(metricsRegistry instanceof NoopMetricsRegistry);
        assertTrue(counter instanceof NoopCounter);
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WrappedMetricsRegistryTests extends OpenSearchTestCase {

    public void testCounter() throws Exception {
        Settings settings = Settings.builder().put(TelemetrySettings.METRICS_ENABLED_SETTING.getKey(), false).build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, new ClusterSettings(settings, getClusterSettings()));
        MetricsTelemetry mockMetricsTelemetry = mock(MetricsTelemetry.class);
        Counter mockCounter = mock(Counter.class);
        Counter mockUpDownCounter = mock(Counter.class);
        DefaultMetricsRegistry defaultMeterRegistry = new DefaultMetricsRegistry(mockMetricsTelemetry);

        when(mockMetricsTelemetry.createCounter("test", "test", "test")).thenReturn(mockCounter);
        when(mockMetricsTelemetry.createUpDownCounter("test", "test", "test")).thenReturn(mockUpDownCounter);

        WrappedMetricsRegistry wrappedMeterRegistry = new WrappedMetricsRegistry(telemetrySettings, defaultMeterRegistry);
        assertTrue(wrappedMeterRegistry.createCounter("test", "test", "test") instanceof WrappedCounter);
        assertTrue(wrappedMeterRegistry.createUpDownCounter("test", "test", "test") instanceof WrappedCounter);
    }

    private Set<Setting<?>> getClusterSettings() {
        Set<Setting<?>> allTracerSettings = new HashSet<>();
        ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TELEMETRY)).stream().forEach((allTracerSettings::add));
        return allTracerSettings;
    }

}

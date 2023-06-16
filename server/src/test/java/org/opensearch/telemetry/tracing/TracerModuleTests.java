/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.plugins.TracerPlugin;
import org.opensearch.telemetry.Telemetry;
import org.opensearch.telemetry.metrics.MetricsTelemetry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.telemetry.tracing.TracerModule.TRACER_DEFAULT_TYPE_SETTING;

public class TracerModuleTests extends OpenSearchTestCase {

    public void testGetTelemetrySupplier() {
        Settings settings = Settings.builder().put(TRACER_DEFAULT_TYPE_SETTING.getKey(), "otel").build();
        TracerSettings tracerSettings = new TracerSettings(settings, new ClusterSettings(settings, getClusterSettings()));
        TracerPlugin tracerPlugin1 = mock(TracerPlugin.class);
        TracerPlugin tracerPlugin2 = mock(TracerPlugin.class);
        TracingTelemetry tracingTelemetry1 = mock(TracingTelemetry.class);
        TracingTelemetry tracingTelemetry2 = mock(TracingTelemetry.class);
        when(tracerPlugin1.getTelemetries(tracerSettings)).thenReturn(Map.of("otel", () -> new Telemetry() {
            @Override
            public TracingTelemetry getTracingTelemetry() {
                return tracingTelemetry1;
            }

            @Override
            public MetricsTelemetry getMetricsTelemetry() {
                return null;
            }
        }));
        when(tracerPlugin2.getTelemetries(tracerSettings)).thenReturn(Map.of("foo", () -> new Telemetry() {
            @Override
            public TracingTelemetry getTracingTelemetry() {
                return tracingTelemetry2;
            }

            @Override
            public MetricsTelemetry getMetricsTelemetry() {
                return null;
            }
        }));
        List<TracerPlugin> tracerPlugins = List.of(tracerPlugin1, tracerPlugin2);

        TracerModule tracerModule = new TracerModule(settings, tracerPlugins, tracerSettings);

        assertEquals(tracingTelemetry1, tracerModule.getTelemetrySupplier().get().getTracingTelemetry());
    }

    private Set<Setting<?>> getClusterSettings() {
        Set<Setting<?>> allTracerSettings = new HashSet<>();
        ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TRACER)).stream().forEach((allTracerSettings::add));
        return allTracerSettings;
    }
}

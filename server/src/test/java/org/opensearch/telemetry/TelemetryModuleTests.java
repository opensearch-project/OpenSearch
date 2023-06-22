/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.plugins.TelemetryPlugin;
import org.opensearch.telemetry.metrics.MetricsTelemetry;
import org.opensearch.telemetry.tracing.TracingTelemetry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.telemetry.TelemetryModule.TELEMETRY_DEFAULT_TYPE_SETTING;

public class TelemetryModuleTests extends OpenSearchTestCase {

    public void testGetTelemetryDefault() {
        Settings settings = Settings.builder().put(TELEMETRY_DEFAULT_TYPE_SETTING.getKey(), "otel").build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, new ClusterSettings(settings, getClusterSettings()));
        TelemetryPlugin telemetryPlugin1 = mock(TelemetryPlugin.class);
        TracingTelemetry tracingTelemetry1 = mock(TracingTelemetry.class);
        when(telemetryPlugin1.getTelemetry(telemetrySettings)).thenReturn(Optional.of(new Telemetry() {
            @Override
            public TracingTelemetry getTracingTelemetry() {
                return tracingTelemetry1;
            }

            @Override
            public MetricsTelemetry getMetricsTelemetry() {
                return null;
            }
        }));
        when(telemetryPlugin1.getName()).thenReturn("otel");
        List<TelemetryPlugin> telemetryPlugins = List.of(telemetryPlugin1);

        TelemetryModule telemetryModule = new TelemetryModule(settings, telemetryPlugins, telemetrySettings);

        assertEquals(tracingTelemetry1, telemetryModule.getTelemetry().getTracingTelemetry());
    }

    public void testGetTelemetryCustom() {
        Settings settings = Settings.builder().put(TELEMETRY_DEFAULT_TYPE_SETTING.getKey(), "otel").build();
        Set<Setting<?>> clusterSettings = getClusterSettings();
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, new ClusterSettings(settings, clusterSettings));
        TelemetryPlugin telemetryPlugin1 = mock(TelemetryPlugin.class);
        TelemetryPlugin telemetryPlugin2 = mock(TelemetryPlugin.class);
        TracingTelemetry tracingTelemetry1 = mock(TracingTelemetry.class);
        TracingTelemetry tracingTelemetry2 = mock(TracingTelemetry.class);
        when(telemetryPlugin1.getTelemetry(telemetrySettings)).thenReturn(Optional.of(new Telemetry() {
            @Override
            public TracingTelemetry getTracingTelemetry() {
                return tracingTelemetry1;
            }

            @Override
            public MetricsTelemetry getMetricsTelemetry() {
                return null;
            }
        }));
        when(telemetryPlugin1.getName()).thenReturn("otel");
        when(telemetryPlugin2.getTelemetry(telemetrySettings)).thenReturn(Optional.of(new Telemetry() {
            @Override
            public TracingTelemetry getTracingTelemetry() {
                return tracingTelemetry2;
            }

            @Override
            public MetricsTelemetry getMetricsTelemetry() {
                return null;
            }
        }));
        when(telemetryPlugin2.getName()).thenReturn("foo");
        List<TelemetryPlugin> telemetryPlugins = List.of(telemetryPlugin1, telemetryPlugin2);

        TelemetryModule telemetryModule = new TelemetryModule(settings, telemetryPlugins, telemetrySettings);

        assertEquals(tracingTelemetry2, telemetryModule.getTelemetry().getTracingTelemetry());
    }

    public void testGetTelemetryWithMoreThanOneCustomTelemetry() {
        Settings settings = Settings.builder().put(TELEMETRY_DEFAULT_TYPE_SETTING.getKey(), "otel").build();
        Set<Setting<?>> clusterSettings = getClusterSettings();
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, new ClusterSettings(settings, clusterSettings));
        TelemetryPlugin telemetryPlugin1 = mock(TelemetryPlugin.class);
        TelemetryPlugin telemetryPlugin2 = mock(TelemetryPlugin.class);
        TelemetryPlugin telemetryPlugin3 = mock(TelemetryPlugin.class);
        TracingTelemetry tracingTelemetry1 = mock(TracingTelemetry.class);
        TracingTelemetry tracingTelemetry2 = mock(TracingTelemetry.class);
        TracingTelemetry tracingTelemetry3 = mock(TracingTelemetry.class);
        when(telemetryPlugin1.getTelemetry(telemetrySettings)).thenReturn(Optional.of(new Telemetry() {
            @Override
            public TracingTelemetry getTracingTelemetry() {
                return tracingTelemetry1;
            }

            @Override
            public MetricsTelemetry getMetricsTelemetry() {
                return null;
            }
        }));
        when(telemetryPlugin1.getName()).thenReturn("otel");
        when(telemetryPlugin2.getTelemetry(telemetrySettings)).thenReturn(Optional.of(new Telemetry() {
            @Override
            public TracingTelemetry getTracingTelemetry() {
                return tracingTelemetry2;
            }

            @Override
            public MetricsTelemetry getMetricsTelemetry() {
                return null;
            }
        }));
        when(telemetryPlugin2.getName()).thenReturn("foo");
        when(telemetryPlugin3.getTelemetry(telemetrySettings)).thenReturn(Optional.of(new Telemetry() {
            @Override
            public TracingTelemetry getTracingTelemetry() {
                return tracingTelemetry3;
            }

            @Override
            public MetricsTelemetry getMetricsTelemetry() {
                return null;
            }
        }));
        when(telemetryPlugin3.getName()).thenReturn("bar");
        try {
            List<TelemetryPlugin> telemetryPlugins = List.of(telemetryPlugin1, telemetryPlugin2, telemetryPlugin3);
            TelemetryModule telemetryModule = new TelemetryModule(settings, telemetryPlugins, telemetrySettings);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertEquals("Cannot register more than one custom telemetry", e.getMessage());
        }

    }

    private Set<Setting<?>> getClusterSettings() {
        Set<Setting<?>> allTracerSettings = new HashSet<>();
        ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TELEMETRY)).stream().forEach((allTracerSettings::add));

        return allTracerSettings;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.telemetry.tracing.OtelTracingTelemetry;
import org.opensearch.telemetry.tracing.TracingTelemetry;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Optional;

import static org.opensearch.telemetry.OTelTelemetryModulePlugin.OTEL_TRACER_NAME;

public class OTelTelemetryModulePluginTests extends OpenSearchTestCase {

    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testAdditionalSettingWithTracingFeatureDisabled() {
        System.setProperty("opensearch.experimental.feature.telemetry.enabled", "false");
        Settings settings = new OTelTelemetryModulePlugin().additionalSettings();

        assertTrue(settings.isEmpty());
    }

    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testAdditionalSettingWithTracingFeatureEnabled() {
        System.setProperty("opensearch.experimental.feature.telemetry.enabled", "true");
        Settings settings = new OTelTelemetryModulePlugin().additionalSettings();

        assertFalse(settings.isEmpty());
    }

    public void testGetTelemetry() throws IOException {
        Set<Setting<?>> allTracerSettings = new HashSet<>();
        ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TELEMETRY)).stream().forEach((allTracerSettings::add));
        Settings settings = Settings.builder().build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, allTracerSettings);
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, clusterSettings);
        OTelTelemetryModulePlugin oTelTracerModulePlugin = new OTelTelemetryModulePlugin();
        Optional<Telemetry> tracer = oTelTracerModulePlugin.getTelemetry(telemetrySettings);

        assertEquals(OTEL_TRACER_NAME, oTelTracerModulePlugin.getName());
        TracingTelemetry tracingTelemetry = tracer.get().getTracingTelemetry();
        assertTrue(tracingTelemetry instanceof OtelTracingTelemetry);
        tracingTelemetry.close();

    }

}

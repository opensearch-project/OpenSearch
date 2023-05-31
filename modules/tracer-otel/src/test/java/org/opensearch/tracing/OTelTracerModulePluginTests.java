/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.opensearch.tracing.OTelTracerModulePlugin.OTEL_TRACER_NAME;

public class OTelTracerModulePluginTests extends OpenSearchTestCase {

    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testAdditionalSettingWithTracingFeatureDisabled() {
        System.setProperty("opensearch.experimental.feature.tracer.enabled", "false");
        Settings settings = new OTelTracerModulePlugin().additionalSettings();

        assertTrue(settings.isEmpty());
    }

    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testAdditionalSettingWithTracingFeatureEnabled() {
        System.setProperty("opensearch.experimental.feature.tracer.enabled", "true");
        Settings settings = new OTelTracerModulePlugin().additionalSettings();

        assertFalse(settings.isEmpty());
    }

    public void testGetTracers() throws IOException {
        Set<Setting<?>> allTracerSettings = new HashSet<>();
        ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TRACER)).stream().forEach((allTracerSettings::add));
        Settings settings = Settings.builder().put(TracerSettings.TRACER_LEVEL_SETTING.getKey(), Level.INFO).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, allTracerSettings);
        TracerSettings tracerSettings = new TracerSettings(settings, clusterSettings);
        Map<String, Supplier<Telemetry>> tracers = new OTelTracerModulePlugin().getTelemetries(tracerSettings);

        assertEquals(Set.of(OTEL_TRACER_NAME), tracers.keySet());
        Telemetry telemetry = tracers.get(OTEL_TRACER_NAME).get();
        assertTrue(telemetry instanceof OtelTelemetry);
        telemetry.close();

    }

}

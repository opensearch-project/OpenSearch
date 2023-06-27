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
import org.opensearch.telemetry.tracing.OTelTracingTelemetry;
import org.opensearch.telemetry.tracing.TracingTelemetry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Optional;

import static org.opensearch.telemetry.OTelTelemetryPlugin.OTEL_TRACER_NAME;
import static org.opensearch.telemetry.OTelTelemetryPlugin.TRACER_EXPORTER_BATCH_SIZE_SETTING;
import static org.opensearch.telemetry.OTelTelemetryPlugin.TRACER_EXPORTER_DELAY_SETTING;
import static org.opensearch.telemetry.OTelTelemetryPlugin.TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING;

public class OTelTelemetryPluginTests extends OpenSearchTestCase {

    public void testGetTelemetry() {
        Set<Setting<?>> allTracerSettings = new HashSet<>();
        ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TELEMETRY)).stream().forEach((allTracerSettings::add));
        Settings settings = Settings.builder().build();
        OTelTelemetryPlugin oTelTracerModulePlugin = new OTelTelemetryPlugin(settings);
        Optional<Telemetry> tracer = oTelTracerModulePlugin.getTelemetry(null);

        assertEquals(OTEL_TRACER_NAME, oTelTracerModulePlugin.getName());
        TracingTelemetry tracingTelemetry = tracer.get().getTracingTelemetry();
        assertTrue(tracingTelemetry instanceof OTelTracingTelemetry);
        assertEquals(
            Arrays.asList(TRACER_EXPORTER_BATCH_SIZE_SETTING, TRACER_EXPORTER_DELAY_SETTING, TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING),
            oTelTracerModulePlugin.getSettings()
        );
        tracingTelemetry.close();

    }

}

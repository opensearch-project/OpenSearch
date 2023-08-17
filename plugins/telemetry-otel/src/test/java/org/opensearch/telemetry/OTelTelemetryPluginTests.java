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
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.telemetry.tracing.OTelTracingTelemetry;
import org.opensearch.telemetry.tracing.TracingTelemetry;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.telemetry.OTelTelemetryPlugin.OTEL_TRACER_NAME;

public class OTelTelemetryPluginTests extends OpenSearchTestCase {

    private OTelTelemetryPlugin oTelTracerModulePlugin;
    private Optional<Telemetry> telemetry;
    private TracingTelemetry tracingTelemetry;

    @Before
    public void setup() {
        // TRACER_EXPORTER_DELAY_SETTING should always be less than 10 seconds because
        // io.opentelemetry.sdk.OpenTelemetrySdk.close waits only for 10 seconds for shutdown to complete.
        oTelTracerModulePlugin = new OTelTelemetryPlugin();
        telemetry = oTelTracerModulePlugin.getTelemetry();
        tracingTelemetry = telemetry.get().getTracingTelemetry();
    }

    public void testGetTelemetry() {
        Set<Setting<?>> allTracerSettings = new HashSet<>();
        ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TELEMETRY)).stream().forEach((allTracerSettings::add));
        assertEquals(OTEL_TRACER_NAME, oTelTracerModulePlugin.getName());
        assertTrue(tracingTelemetry instanceof OTelTracingTelemetry);
    }

    @After
    public void cleanup() {
        tracingTelemetry.close();
    }
}

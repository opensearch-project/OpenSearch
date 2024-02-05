/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.telemetry.tracing.sampler;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

import io.opentelemetry.sdk.trace.samplers.Sampler;

import static org.opensearch.telemetry.OTelTelemetrySettings.OTEL_TRACER_SPAN_SAMPLER_CLASS_SETTINGS;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_ENABLED_SETTING;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_SAMPLER_ACTION_PROBABILITY;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_SAMPLER_PROBABILITY;

public class OTelSamplerFactoryTests extends OpenSearchTestCase {

    public void testCreate() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_SAMPLER_ACTION_PROBABILITY)
        );
        TelemetrySettings telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);

        List<Sampler> samplersList = OTelSamplerFactory.create(telemetrySettings, Settings.EMPTY);

        List<String> samplersNameList = OTEL_TRACER_SPAN_SAMPLER_CLASS_SETTINGS.get(Settings.EMPTY);

        for (Sampler sampler : samplersList) {
            assertTrue(samplersNameList.contains(sampler.getClass().getName()));
        }
    }
}

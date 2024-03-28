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
import org.opensearch.telemetry.OTelTelemetrySettings;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

import io.opentelemetry.sdk.trace.samplers.Sampler;

import static org.opensearch.telemetry.TelemetrySettings.TRACER_ENABLED_SETTING;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_INFERRED_SAMPLER_ALLOWLISTED;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_SAMPLER_PROBABILITY;

public class OTelSamplerFactoryTests extends OpenSearchTestCase {

    public void testDefaultCreate() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_INFERRED_SAMPLER_ALLOWLISTED)
        );
        TelemetrySettings telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);
        Sampler sampler = OTelSamplerFactory.create(telemetrySettings, Settings.EMPTY);
        assertEquals(sampler.getClass(), ProbabilisticTransportActionSampler.class);
    }

    public void testCreateWithSingleSampler() {
        Settings settings = Settings.builder()
            .put(OTelTelemetrySettings.OTEL_TRACER_SPAN_SAMPLER_CLASS_SETTINGS.getKey(), ProbabilisticSampler.class.getName())
            .build();

        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_INFERRED_SAMPLER_ALLOWLISTED)
        );
        TelemetrySettings telemetrySettings = new TelemetrySettings(settings, clusterSettings);
        Sampler sampler = OTelSamplerFactory.create(telemetrySettings, settings);
        assertTrue(sampler instanceof ProbabilisticSampler);
    }
}

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

import java.util.Set;

import io.opentelemetry.sdk.trace.samplers.Sampler;

import static org.opensearch.telemetry.OTelTelemetrySettings.TRACER_EXPORTER_DELAY_SETTING;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_ENABLED_SETTING;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_SAMPLER_PROBABILITY;

public class ProbabilisticSamplerTests extends OpenSearchTestCase {

    // When ProbabilisticSampler is created with OTelTelemetrySettings as null
    public void testProbabilisticSamplerWithNullSettings() {
        // Verify that the constructor throws IllegalArgumentException when given null settings
        assertThrows(NullPointerException.class, () -> { new ProbabilisticSampler(null); });
    }

    public void testDefaultGetSampler() {
        Settings settings = Settings.builder().put(TRACER_EXPORTER_DELAY_SETTING.getKey(), "1s").build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(
            Settings.EMPTY,
            new ClusterSettings(settings, Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING))
        );

        // Probabilistic Sampler
        ProbabilisticSampler probabilisticSampler = new ProbabilisticSampler(telemetrySettings);

        assertNotNull(probabilisticSampler.getSampler());
        assertEquals(0.01, probabilisticSampler.getSamplingRatio(), 0.0d);
    }

    public void testGetSamplerWithUpdatedSamplingRatio() {
        Settings settings = Settings.builder().put(TRACER_EXPORTER_DELAY_SETTING.getKey(), "1s").build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(
            Settings.EMPTY,
            new ClusterSettings(settings, Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING))
        );

        // Probabilistic Sampler
        ProbabilisticSampler probabilisticSampler = new ProbabilisticSampler(telemetrySettings);
        assertEquals(0.01d, probabilisticSampler.getSamplingRatio(), 0.0d);

        telemetrySettings.setSamplingProbability(0.02);

        // Need to call getSampler() to update the value of tracerHeadSamplerSamplingRatio
        Sampler updatedProbabilisticSampler = probabilisticSampler.getSampler();
        assertEquals(0.02, probabilisticSampler.getSamplingRatio(), 0.0d);
    }

}

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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.internal.AttributesMap;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import static org.opensearch.telemetry.OTelTelemetrySettings.TRACER_EXPORTER_DELAY_SETTING;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_ENABLED_SETTING;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_SAMPLER_ACTION_PROBABILITY;
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
            new ClusterSettings(settings, Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_SAMPLER_ACTION_PROBABILITY))
        );

        AttributesMap attributes = AttributesMap.create(1, 100);

        // Probabilistic Sampler
        ProbabilisticSampler probabilisticSampler = new ProbabilisticSampler(telemetrySettings);

        assertNotNull(probabilisticSampler.getSampler(attributes));
        assertEquals(0.01, probabilisticSampler.getSamplingRatio(), 0.0d);
    }

    public void testGetSamplerWithUpdatedSamplingRatio() {
        Settings settings = Settings.builder().put(TRACER_EXPORTER_DELAY_SETTING.getKey(), "1s").build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(
            Settings.EMPTY,
            new ClusterSettings(settings, Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_SAMPLER_ACTION_PROBABILITY))
        );

        AttributesMap attributes = AttributesMap.create(1, 100);

        // Probabilistic Sampler
        ProbabilisticSampler probabilisticSampler = new ProbabilisticSampler(telemetrySettings);

        assertNotNull(probabilisticSampler.getSampler(attributes));
        assertEquals(0.01d, probabilisticSampler.getSamplingRatio(), 0.0d);

        telemetrySettings.setSamplingProbability(0.02);

        // Need to call getSampler() to update the value of tracerHeadSamplerSamplingRatio
        Sampler updatedProbabilisticSampler = probabilisticSampler.getSampler(attributes);
        assertEquals(0.02, probabilisticSampler.getSamplingRatio(), 0.0d);
    }

    public void testGetSamplerWithCustomActionSamplingRatio() {
        Settings settings = Settings.builder().put(TRACER_EXPORTER_DELAY_SETTING.getKey(), "1s").build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(
            Settings.EMPTY,
            new ClusterSettings(settings, Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_SAMPLER_ACTION_PROBABILITY))
        );
        telemetrySettings.setSamplingProbability(0.02);
        AttributesMap attributes = AttributesMap.create(1, 100);

        // Probabilistic Sampler
        ProbabilisticSampler probabilisticSampler = new ProbabilisticSampler(telemetrySettings);

        Map<String, Double> filters = new HashMap<>();
        // Setting 100% sampling for cluster/coordination/join request
        filters.put("internal:cluster/coordination/join", 1.00);
        telemetrySettings.setActionSamplingProbability(filters);

        attributes.put(AttributeKey.stringKey("action"), "internal:cluster/coordination/join");
        probabilisticSampler.getSampler(attributes);

        // Validates sampling probability for cluster coordination request as override is present for it.
        assertEquals(1.00, probabilisticSampler.getActionSamplingRatio("internal:cluster/coordination/join"), 0.0d);
        // Validates sampling probability for cluster coordination request second call.
        assertEquals(1.00, probabilisticSampler.getActionSamplingRatio("internal:cluster/coordination/join"), 0.0d);

        // Updating sampling ratio to 30%
        filters.put("internal:cluster/coordination/join", 0.30);
        telemetrySettings.setActionSamplingProbability(filters);
        // Need to call getSampler() to update the value of tracerHeadSamplerSamplingRatio
        probabilisticSampler.getSampler(attributes);

        // Validates sampling probability for cluster coordination as override is present for it.
        assertEquals(0.30, probabilisticSampler.getActionSamplingRatio("internal:cluster/coordination/join"), 0.0d);
    }

}

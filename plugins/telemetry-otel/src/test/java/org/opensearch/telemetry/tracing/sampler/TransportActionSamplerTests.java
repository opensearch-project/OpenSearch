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

import java.util.Collections;
import java.util.Set;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;

import static org.opensearch.telemetry.OTelTelemetrySettings.TRACER_EXPORTER_DELAY_SETTING;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_ENABLED_SETTING;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_SAMPLER_ACTION_PROBABILITY;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_SAMPLER_PROBABILITY;
import static org.opensearch.telemetry.tracing.AttributeNames.TRANSPORT_ACTION;
import static org.mockito.Mockito.mock;

public class TransportActionSamplerTests extends OpenSearchTestCase {

    public void testGetSamplerWithDefaultActionSamplingRatio() {
        Settings settings = Settings.builder().put(TRACER_EXPORTER_DELAY_SETTING.getKey(), "1s").build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(
            Settings.EMPTY,
            new ClusterSettings(settings, Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_SAMPLER_ACTION_PROBABILITY))
        );

        // TransportActionSampler
        TransportActionSampler transportActionSampler = new TransportActionSampler(telemetrySettings);

        // Validates if default sampling of 1%
        assertEquals(0.001d, transportActionSampler.getSamplingRatio(), 0.0d);
    }

    public void testGetSamplerWithUpdatedActionSamplingRatio() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_SAMPLER_ACTION_PROBABILITY)
        );

        TelemetrySettings telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);

        // TransportActionSampler
        TransportActionSampler transportActionSampler = new TransportActionSampler(telemetrySettings);
        clusterSettings.applySettings(Settings.builder().put("telemetry.tracer.action.sampler.probability", "1.0").build());

        // Need to call shouldSample() to update the value of samplingRatio
        SamplingResult result = transportActionSampler.shouldSample(
            mock(Context.class),
            "00000000000000000000000000000000",
            "spanName",
            SpanKind.INTERNAL,
            Attributes.builder().put(TRANSPORT_ACTION, "dummy_action").build(),
            Collections.emptyList()
        );
        // Verify that TransportActionSampler returned SamplingResult.recordAndSample() as all actions will be sampled
        assertEquals(SamplingResult.recordAndSample(), result);
        assertEquals(1.0, transportActionSampler.getSamplingRatio(), 0.000d);

        clusterSettings.applySettings(Settings.builder().put("telemetry.tracer.action.sampler.probability", "0.5").build());
        result = transportActionSampler.shouldSample(
            mock(Context.class),
            "00000000000000000000000000000000",
            "spanName",
            SpanKind.INTERNAL,
            Attributes.builder().put(TRANSPORT_ACTION, "dummy_action").build(),
            Collections.emptyList()
        );
        assertEquals(0.5, transportActionSampler.getSamplingRatio(), 0.000d);
    }

}

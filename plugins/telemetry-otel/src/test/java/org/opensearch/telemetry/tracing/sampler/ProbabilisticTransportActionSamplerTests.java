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

import static org.opensearch.telemetry.TelemetrySettings.TRACER_ENABLED_SETTING;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_SAMPLER_ACTION_PROBABILITY;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_SAMPLER_PROBABILITY;
import static org.opensearch.telemetry.tracing.AttributeNames.TRANSPORT_ACTION;
import static org.mockito.Mockito.mock;

public class ProbabilisticTransportActionSamplerTests extends OpenSearchTestCase {

    public void testGetSamplerWithUpdatingActionSamplingRatio() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_SAMPLER_ACTION_PROBABILITY)
        );

        TelemetrySettings telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);

        // TransportActionSampler
        ProbabilisticTransportActionSampler probabilisticTransportActionSampler = new ProbabilisticTransportActionSampler(
            telemetrySettings
        );
        clusterSettings.applySettings(Settings.builder().put("telemetry.tracer.action.sampler.probability", "1.0").build());

        // Need to call shouldSample() to update the value of samplingRatio
        SamplingResult result = probabilisticTransportActionSampler.shouldSample(
            mock(Context.class),
            "00000000000000000000000000000000",
            "spanName",
            SpanKind.INTERNAL,
            Attributes.builder().put(TRANSPORT_ACTION, "dummy_action").build(),
            Collections.emptyList()
        );
        // Verify that TransportActionSampler returned SamplingResult.recordAndSample() as all actions will be sampled
        assertEquals(SamplingResult.recordAndSample(), result);
        assertEquals(1.0, probabilisticTransportActionSampler.getSamplingRatio(), 0.000d);
        assertEquals(SamplingResult.recordAndSample(), result);

        clusterSettings.applySettings(Settings.builder().put("telemetry.tracer.action.sampler.probability", "0.0").build());
        result = probabilisticTransportActionSampler.shouldSample(
            mock(Context.class),
            "00000000000000000000000000000000",
            "spanName",
            SpanKind.INTERNAL,
            Attributes.builder().put(TRANSPORT_ACTION, "dummy_action").build(),
            Collections.emptyList()
        );
        assertEquals(0.0, probabilisticTransportActionSampler.getSamplingRatio(), 0.000d);
        assertEquals(SamplingResult.drop(), result);
    }
}

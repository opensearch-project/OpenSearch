/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.samplingResult;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.telemetry.tracing.attributes.SamplingAttributes;
import org.opensearch.telemetry.tracing.sampler.InferredActionSampler;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.Set;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;

import static org.opensearch.telemetry.TelemetrySettings.TRACER_ENABLED_SETTING;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_INFERRED_SAMPLER_ALLOWLISTED;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_SAMPLER_PROBABILITY;
import static org.opensearch.telemetry.tracing.AttributeNames.TRANSPORT_ACTION;
import static org.mockito.Mockito.mock;

public class OTelSamplingResultTests extends OpenSearchTestCase {

    public void testSamplingResult() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_INFERRED_SAMPLER_ALLOWLISTED)
        );

        TelemetrySettings telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);
        telemetrySettings.setInferredSamplingAllowListed(true);

        // InferredActionSampler
        Sampler inferredActionSampler = InferredActionSampler.create(telemetrySettings, Settings.EMPTY, null);

        SamplingResult result = inferredActionSampler.shouldSample(
            mock(Context.class),
            "00000000000000000000000000000000",
            "spanName",
            SpanKind.INTERNAL,
            Attributes.builder().put(TRANSPORT_ACTION, "dummy_action").build(),
            Collections.emptyList()
        );

        assertTrue(result.getAttributes().asMap().containsKey(AttributeKey.stringKey(SamplingAttributes.SAMPLER.getValue())));
        assertEquals(result.getClass(), OTelSamplingResult.class);
    }

    public void testSamplingDecisionAndAttributes() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_INFERRED_SAMPLER_ALLOWLISTED)
        );

        TelemetrySettings telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);
        telemetrySettings.setInferredSamplingAllowListed(true);

        // InferredActionSampler
        Sampler inferredActionSampler = InferredActionSampler.create(telemetrySettings, Settings.EMPTY, null);

        SamplingResult result = inferredActionSampler.shouldSample(
            mock(Context.class),
            "00000000000000000000000000000000",
            "spanName",
            SpanKind.INTERNAL,
            Attributes.builder().put(TRANSPORT_ACTION, "dummy_action").build(),
            Collections.emptyList()
        );

        OTelSamplingResult oTelSamplingResult = new OTelSamplingResult(result.getDecision(), result.getAttributes());
        assertEquals(result.getDecision(), oTelSamplingResult.getDecision());
        assertEquals(result.getAttributes(), oTelSamplingResult.getAttributes());
    }
}

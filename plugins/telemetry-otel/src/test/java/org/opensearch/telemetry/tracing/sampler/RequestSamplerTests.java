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

import io.opentelemetry.api.common.AttributeKey;
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

public class RequestSamplerTests extends OpenSearchTestCase {

    public void testShouldSampleWithTraceAttributeAsTrue() {
        Settings settings = Settings.builder().put(TRACER_EXPORTER_DELAY_SETTING.getKey(), "1s").build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(
            Settings.EMPTY,
            new ClusterSettings(settings, Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_SAMPLER_ACTION_PROBABILITY))
        );
        // Create an instance of requestSampler
        RequestSampler requestSampler = new RequestSampler(telemetrySettings);

        // Create a mock Context and Attributes with trace as true
        Context parentContext = mock(Context.class);
        Attributes attributes = Attributes.of(AttributeKey.stringKey("trace"), "true");

        // Call shouldSample on HeadSampler
        SamplingResult result = requestSampler.shouldSample(
            parentContext,
            "traceId",
            "spanName",
            SpanKind.INTERNAL,
            attributes,
            Collections.emptyList()
        );

        assertEquals(SamplingResult.recordAndSample(), result);
    }

    public void testShouldSampleWithTraceAttributeAsFalse() {
        Settings settings = Settings.builder().put(TRACER_EXPORTER_DELAY_SETTING.getKey(), "1s").build();
        TelemetrySettings telemetrySettings = new TelemetrySettings(
            Settings.EMPTY,
            new ClusterSettings(settings, Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_SAMPLER_ACTION_PROBABILITY))
        );

        // Create an instance of requestSampler
        RequestSampler requestSampler = new RequestSampler(telemetrySettings);

        // Create a mock Context and Attributes with trace as false
        Context parentContext = mock(Context.class);
        Attributes attributes = Attributes.of(AttributeKey.stringKey("trace"), "false");

        // Call shouldSample on HeadSampler
        SamplingResult result = requestSampler.shouldSample(
            parentContext,
            "traceId",
            "spanName",
            SpanKind.INTERNAL,
            attributes,
            Collections.emptyList()
        );
        assertEquals(SamplingResult.drop(), result);
    }

    public void testShouldSampleWithoutTraceAttribute() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_SAMPLER_ACTION_PROBABILITY)
        );
        TelemetrySettings telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);

        // Create an instance of requestSampler
        RequestSampler requestSampler = new RequestSampler(telemetrySettings);

        clusterSettings.applySettings(Settings.builder().put("telemetry.tracer.action.sampler.probability", "1.0").build());

        // Create a mock Context and Attributes with dummy action
        Context parentContext = mock(Context.class);
        Attributes attributes = Attributes.builder().put(TRANSPORT_ACTION, "dummy_action").build();

        // Calling shouldSample to update samplingRatio
        SamplingResult result = requestSampler.shouldSample(
            parentContext,
            "00000000000000000000000000000000",
            "spanName",
            SpanKind.INTERNAL,
            attributes,
            Collections.emptyList()
        );

        // Verify that request is sampled
        assertEquals(SamplingResult.recordAndSample(), result);

        // Verify that sampler dropped the request
        clusterSettings.applySettings(Settings.builder().put("telemetry.tracer.action.sampler.probability", "0.0").build());
        result = requestSampler.shouldSample(
            parentContext,
            "00000000000000000000000000000000",
            "spanName",
            SpanKind.INTERNAL,
            attributes,
            Collections.emptyList()
        );
        assertEquals(SamplingResult.drop(), result);

        // Verify that request is sampled when probability is set to 1
        clusterSettings.applySettings(Settings.builder().put("telemetry.tracer.sampler.probability", "1.0").build());
        result = requestSampler.shouldSample(
            parentContext,
            "00000000000000000000000000000000",
            "spanName",
            SpanKind.INTERNAL,
            Attributes.empty(),
            Collections.emptyList()
        );
        assertEquals(SamplingResult.recordAndSample(), result);

        // Verify that request is not sampled when probability is set to 0
        clusterSettings.applySettings(Settings.builder().put("telemetry.tracer.sampler.probability", "0.0").build());
        result = requestSampler.shouldSample(
            parentContext,
            "00000000000000000000000000000000",
            "spanName",
            SpanKind.INTERNAL,
            Attributes.empty(),
            Collections.emptyList()
        );
        assertEquals(SamplingResult.drop(), result);

    }

}

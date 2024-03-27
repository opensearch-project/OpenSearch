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
import org.junit.Before;

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

public class RequestSamplerTests extends OpenSearchTestCase {
    private ClusterSettings clusterSettings;
    private TelemetrySettings telemetrySettings;
    private RequestSampler requestSampler;
    private Context parentContext;

    @Before
    public void init() {
        clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(TRACER_SAMPLER_PROBABILITY, TRACER_ENABLED_SETTING, TRACER_INFERRED_SAMPLER_ALLOWLISTED)
        );
        telemetrySettings = new TelemetrySettings(Settings.EMPTY, clusterSettings);
        Sampler fallbackSampler = OTelSamplerFactory.create(telemetrySettings, Settings.EMPTY);
        requestSampler = new RequestSampler(fallbackSampler);
        parentContext = mock(Context.class);
    }

    public void testShouldSampleWithTraceAttributeAsTrue() {
        Attributes attributes = Attributes.of(AttributeKey.stringKey("trace"), "true");

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
        Attributes attributes = Attributes.of(AttributeKey.stringKey("trace"), "false");

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

    public void testShouldSampleForProbabilisticSampler() {
        clusterSettings.applySettings(
            Settings.builder()
                .put("telemetry.tracer.sampler.probability", "1.0")
                .put("telemetry.otel.tracer.span.sampler.classes", "org.opensearch.telemetry.tracing.sampler.ProbabilisticSampler")
                .build()
        );

        Attributes attributes = Attributes.builder().build();

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

        clusterSettings.applySettings(Settings.builder().put("telemetry.tracer.sampler.probability", "0.0").build());
        result = requestSampler.shouldSample(
            parentContext,
            "00000000000000000000000000000000",
            "spanName",
            SpanKind.INTERNAL,
            attributes,
            Collections.emptyList()
        );
        assertEquals(SamplingResult.drop(), result);

    }

    public void testShouldSampleForProbabilisticTransportActionSampler() {
        clusterSettings.applySettings(
            Settings.builder()
                .put(
                    "telemetry.otel.tracer.span.sampler.classes",
                    "org.opensearch.telemetry.tracing.sampler.ProbabilisticTransportActionSampler"
                )
                .build()
        );
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
    }

}

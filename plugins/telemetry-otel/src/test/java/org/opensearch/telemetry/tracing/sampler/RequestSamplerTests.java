/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.sampler;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RequestSamplerTests extends OpenSearchTestCase {

    public void testShouldSampleWithTraceAttributeAsTrue() {

        // Create a mock default sampler
        Sampler defaultSampler = mock(Sampler.class);
        when(defaultSampler.shouldSample(any(), anyString(), anyString(), any(), any(), any())).thenReturn(SamplingResult.drop());

        // Create an instance of HeadSampler with the mock default sampler
        RequestSampler requestSampler = new RequestSampler(defaultSampler);

        // Create a mock Context and Attributes
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

        // Verify that the default sampler's shouldSample method was not called
        verify(defaultSampler, never()).shouldSample(any(), anyString(), anyString(), any(), any(), any());
    }

    public void testShouldSampleWithoutTraceAttribute() {

        // Create a mock default sampler
        Sampler defaultSampler = mock(Sampler.class);
        when(defaultSampler.shouldSample(any(), anyString(), anyString(), any(), any(), any())).thenReturn(
            SamplingResult.recordAndSample()
        );

        // Create an instance of HeadSampler with the mock default sampler
        RequestSampler requestSampler = new RequestSampler(defaultSampler);

        // Create a mock Context and Attributes
        Context parentContext = mock(Context.class);
        Attributes attributes = Attributes.empty();

        // Call shouldSample on HeadSampler
        SamplingResult result = requestSampler.shouldSample(
            parentContext,
            "traceId",
            "spanName",
            SpanKind.INTERNAL,
            attributes,
            Collections.emptyList()
        );

        // Verify that HeadSampler returned SamplingResult.recordAndSample()
        assertEquals(SamplingResult.recordAndSample(), result);

        // Verify that the default sampler's shouldSample method was called
        verify(defaultSampler).shouldSample(any(), anyString(), anyString(), any(), any(), any());
    }

}

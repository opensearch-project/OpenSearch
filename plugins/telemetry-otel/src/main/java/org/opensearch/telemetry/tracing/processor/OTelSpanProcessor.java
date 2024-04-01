/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.processor;

import org.opensearch.telemetry.tracing.attributes.SamplingAttributes;

import java.util.Objects;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;

/**
 * Implementation of the SpanProcessor and delegates to the configured processor.
 */
public class OTelSpanProcessor implements SpanProcessor {

    private final SpanProcessor delegateProcessor;

    /**
     * *
     * @param delegateProcessor the span processor to which this processor will delegate
     */
    public OTelSpanProcessor(SpanProcessor delegateProcessor) {
        this.delegateProcessor = delegateProcessor;
    }

    /**
     * Called when a {@link Span} is started, if the {@link
     * Span#isRecording()} returns true.
     *
     * <p>This method is called synchronously on the execution thread, should not throw or block the
     * execution thread.
     *
     * @param parentContext the parent {@code Context} of the span that just started.
     * @param span          the {@code Span} that just started.
     */
    @Override
    public void onStart(Context parentContext, ReadWriteSpan span) {
        this.delegateProcessor.onStart(parentContext, span);
    }

    /**
     * Returns {@code true} if this {@link SpanProcessor} requires start events.
     *
     * @return {@code true} if this {@link SpanProcessor} requires start events.
     */
    @Override
    public boolean isStartRequired() {
        return this.delegateProcessor.isStartRequired();
    }

    /**
     * Called when a {@link Span} is ended, if the {@link
     * Span#isRecording()} returns true.
     *
     * <p>This method is called synchronously on the execution thread, should not throw or block the
     * execution thread.
     *
     * @param span the {@code Span} that just ended.
     */
    @Override
    public void onEnd(ReadableSpan span) {
        if (span != null
            && span.getSpanContext().isSampled()
            && Objects.equals(
                span.getAttribute(AttributeKey.stringKey(SamplingAttributes.SAMPLER.getValue())),
                SamplingAttributes.INFERRED_SAMPLER.getValue()
            )) {
            if (span.getAttribute(AttributeKey.booleanKey(SamplingAttributes.SAMPLED.getValue())) != null) {
                this.delegateProcessor.onEnd(span);
            }
        } else {
            this.delegateProcessor.onEnd(span);
        }
    }

    /**
     * Returns {@code true} if this {@link SpanProcessor} requires end events.
     *
     * @return {@code true} if this {@link SpanProcessor} requires end events.
     */
    @Override
    public boolean isEndRequired() {
        return this.delegateProcessor.isEndRequired();
    }

    /**
     * Processes all span events that have not yet been processed and closes used resources.
     *
     * @return a {@link CompletableResultCode} which completes when shutdown is finished.
     */
    @Override
    public CompletableResultCode shutdown() {
        return this.delegateProcessor.shutdown();
    }

    /**
     * Processes all span events that have not yet been processed.
     *
     * @return a {@link CompletableResultCode} which completes when currently queued spans are
     *     finished processing.
     */
    @Override
    public CompletableResultCode forceFlush() {
        return this.delegateProcessor.forceFlush();
    }
}

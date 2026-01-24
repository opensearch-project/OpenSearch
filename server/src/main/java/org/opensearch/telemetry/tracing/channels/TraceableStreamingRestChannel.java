/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.channels;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.http.HttpChunk;
import org.opensearch.rest.StreamingRestChannel;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;

import java.util.List;
import java.util.Map;

import org.reactivestreams.Subscriber;

/**
 * Tracer wrapped {@link StreamingRestChannel} for distributed tracing support.
 *
 * <p>This channel supports two response patterns:
 * <ul>
 *   <li><b>Streaming path:</b> prepareResponse() followed by multiple sendChunk() calls.
 *       Each chunk is sent within its own span scope for proper trace context propagation.</li>
 *   <li><b>Non-streaming path:</b> sendResponse() called directly for complete responses
 *       that don't require chunk-by-chunk streaming.</li>
 * </ul>
 *
 */
class TraceableStreamingRestChannel extends TraceableRestChannel<StreamingRestChannel> implements StreamingRestChannel {

    /**
     * Constructor.
     *
     * @param delegate delegate
     * @param span span
     * @param tracer tracer
     */
    TraceableStreamingRestChannel(StreamingRestChannel delegate, Span span, Tracer tracer) {
        super(delegate, span, tracer);
    }

    @Override
    public void sendChunk(HttpChunk chunk) {
        // Each chunk operation executes within the span scope for proper trace context propagation
        try (SpanScope ignored = tracer.withSpanInScope(span)) {
            delegate.sendChunk(chunk);
        }

        // End the span when the last chunk is sent
        if (chunk.isLast()) {
            span.endSpan();
        }
    }

    @Override
    public void prepareResponse(RestStatus status, Map<String, List<String>> headers) {
        // Prepare response within span scope to ensure proper trace context
        try (SpanScope ignored = tracer.withSpanInScope(span)) {
            delegate.prepareResponse(status, headers);
        }
    }

    @Override
    public boolean isReadable() {
        return delegate.isReadable();
    }

    @Override
    public boolean isWritable() {
        return delegate.isWritable();
    }

    @Override
    public void subscribe(Subscriber<? super HttpChunk> s) {
        delegate.subscribe(s);
    }
}

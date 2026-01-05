/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.channels;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.http.HttpChunk;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.StreamingRestChannel;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * <p><b>Thread Safety:</b> This class is designed to handle concurrent chunk sending from
 * multiple threads (common in reactive streaming scenarios). Each operation establishes
 * its own span scope, and span completion is guarded by an atomic boolean to ensure
 * the span is ended exactly once.
 *
 * @opensearch.internal
 */
public class TraceableStreamingRestChannel implements StreamingRestChannel {

    private final StreamingRestChannel delegate;
    private final Span span;
    private final Tracer tracer;
    private final AtomicBoolean spanEnded = new AtomicBoolean(false);

    /**
     * Constructor.
     *
     * @param delegate delegate
     * @param span span
     * @param tracer tracer
     */
    TraceableStreamingRestChannel(StreamingRestChannel delegate, Span span, Tracer tracer) {
        this.span = Objects.requireNonNull(span);
        this.delegate = Objects.requireNonNull(delegate);
        this.tracer = Objects.requireNonNull(tracer);
    }

    public static RestChannel create(StreamingRestChannel delegate, Span span, Tracer tracer) {
        if (tracer.isRecording() == true) {
            return new TraceableStreamingRestChannel(delegate, span, tracer);
        } else {
            return delegate;
        }
    }

    @Override
    public void sendChunk(HttpChunk chunk) {
        // Each chunk operation executes within the span scope for proper trace context propagation
        try (SpanScope ignored = tracer.withSpanInScope(span)) {
            delegate.sendChunk(chunk);
        }

        // End the span when the last chunk is sent (thread-safe with atomic boolean)
        if (chunk.isLast() && spanEnded.compareAndSet(false, true)) {
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
    public XContentBuilder newBuilder() throws IOException {
        return delegate.newBuilder();
    }

    @Override
    public XContentBuilder newErrorBuilder() throws IOException {
        return delegate.newErrorBuilder();
    }

    @Override
    public XContentBuilder newBuilder(MediaType mediaType, boolean useFiltering) throws IOException {
        return delegate.newBuilder(mediaType, useFiltering);
    }

    @Override
    public XContentBuilder newBuilder(MediaType mediaType, MediaType responseContentType, boolean useFiltering) throws IOException {
        return delegate.newBuilder(mediaType, responseContentType, useFiltering);
    }

    @Override
    public BytesStreamOutput bytesOutput() {
        return delegate.bytesOutput();
    }

    @Override
    public RestRequest request() {
        return delegate.request();
    }

    @Override
    public boolean detailedErrorsEnabled() {
        return delegate.detailedErrorsEnabled();
    }

    @Override
    public boolean detailedErrorStackTraceEnabled() {
        return delegate.detailedErrorStackTraceEnabled();
    }

    @Override
    public void sendResponse(RestResponse response) {
        // Non-streaming path: send complete response within span scope
        try (SpanScope ignored = tracer.withSpanInScope(span)) {
            delegate.sendResponse(response);
        } finally {
            // Ensure span is ended exactly once (may race with sendChunk on last chunk)
            if (spanEnded.compareAndSet(false, true)) {
                span.endSpan();
            }
        }
    }

    @Override
    public void subscribe(Subscriber<? super HttpChunk> s) {
        delegate.subscribe(s);
    }
}

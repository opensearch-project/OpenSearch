/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.channels;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.telemetry.tracing.Span;

import java.io.IOException;
import java.util.Objects;

/**
 * Tracer wrapped {@link RestChannel}
 */
public class TraceableRestChannel implements RestChannel {

    private final RestChannel delegate;
    private final Span span;

    /**
     * Constructor.
     *
     * @param delegate delegate
     * @param span span
     */
    public TraceableRestChannel(RestChannel delegate, Span span) {
        Objects.requireNonNull(delegate);
        Objects.requireNonNull(span);
        this.span = span;
        this.delegate = delegate;
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
    public void sendResponse(RestResponse response) {
        span.endSpan();
        delegate.sendResponse(response);
    }
}

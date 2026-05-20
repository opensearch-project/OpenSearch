/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client.http;

import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.nio.AsyncEntityProducer;
import org.apache.hc.core5.http.nio.support.BasicRequestProducer;
import org.apache.hc.core5.net.URIAuthority;
import org.apache.hc.core5.reactive.ReactiveEntityProducer;
import org.apache.hc.core5.util.Args;

import java.nio.ByteBuffer;

import org.reactivestreams.Publisher;

/**
 * The reactive producer of the {@link HttpUriRequestBase} instances associated with a particular {@link HttpHost}
 */
public class ReactiveHttpUriRequestProducer extends BasicRequestProducer {
    private final HttpUriRequestBase request;

    ReactiveHttpUriRequestProducer(final HttpUriRequestBase request, final AsyncEntityProducer entityProducer) {
        super(request, entityProducer);
        this.request = request;
    }

    /**
     * Get the produced {@link HttpUriRequestBase} instance
     * @return produced {@link HttpUriRequestBase} instance
     */
    public HttpUriRequestBase getRequest() {
        return request;
    }

    /**
     * Create new request producer for {@link HttpUriRequestBase} instance and {@link HttpHost}
     * @param request {@link HttpUriRequestBase} instance
     * @param host {@link HttpHost} instance
     * @param publisher publisher
     * @return new request producer
     */
    public static ReactiveHttpUriRequestProducer create(
        final HttpUriRequestBase request,
        final HttpHost host,
        Publisher<ByteBuffer> publisher
    ) {
        Args.notNull(request, "Request");
        Args.notNull(host, "HttpHost");

        // TODO: Should we copy request here instead of modifying in place?
        request.setAuthority(new URIAuthority(host));
        request.setScheme(host.getSchemeName());

        final Header contentTypeHeader = request.getFirstHeader("Content-Type");
        final ContentType contentType = (contentTypeHeader == null)
            ? ContentType.APPLICATION_JSON
            : ContentType.parse(contentTypeHeader.getValue());

        final Header contentEncodingHeader = request.getFirstHeader("Content-Encoding");
        final String contentEncoding = (contentEncodingHeader == null) ? null : contentEncodingHeader.getValue();

        final AsyncEntityProducer entityProducer = new ReactiveEntityProducer(publisher, -1, contentType, contentEncoding);
        return new ReactiveHttpUriRequestProducer(request, entityProducer);
    }

}

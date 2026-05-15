/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.httpclient;

import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Flow;

import org.reactivestreams.Publisher;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Mono;

/**
 * HTTP Streaming Response from OpenSearch.
 * Note: This is an experimental API.
 */
public class StreamingResponse {
    private final RequestLine requestLine;
    private final Mono<HttpResponse<Flow.Publisher<List<ByteBuffer>>>> publisher;
    private volatile HttpHost host;

    /**
     * Constructor
     * @param requestLine request line
     * @param publisher message publisher(response with a body)
     */
    public StreamingResponse(RequestLine requestLine, Publisher<HttpResponse<Flow.Publisher<List<ByteBuffer>>>> publisher) {
        this.requestLine = requestLine;
        // We cache the publisher here so the body or / and HttpResponse could
        // be consumed independently or/and more than once.
        this.publisher = Mono.from(publisher).cache();
    }

    /**
     * Set host
     * @param host host
     */
    public void setHost(HttpHost host) {
        this.host = host;
    }

    /**
     * Get request line
     * @return request line
     */
    public RequestLine getRequestLine() {
        return requestLine;
    }

    /**
     * Get host
     * @return host
     */
    public HttpHost getHost() {
        return host;
    }

    /**
     * Get response boby {@link Publisher}
     * @return response boby {@link Publisher}
     */
    public Publisher<ByteBuffer> getBody() {
        return publisher.flatMapMany(m -> {
            final boolean compressed = m.headers()
                .firstValue("Content-Encoding")
                .filter("gzip"::equalsIgnoreCase)
                .map(h -> true)
                .orElse(false);
            return JdkFlowAdapter.flowPublisherToFlux(m.body()).flatMapIterable(t -> t).map(b -> {
                if (compressed) {
                    return BodyUtils.decompress(b);
                } else {
                    return b;
                }
            });
        });
    }

    /**
     * Returns the status line of the current response
     */
    @SuppressWarnings("unchecked")
    public StatusLine getStatusLine() {
        return new StatusLine(
            publisher.onErrorResume(
                ResponseException.class,
                e -> Mono.just((HttpResponse<Flow.Publisher<List<ByteBuffer>>>) e.getResponse().getHttpResponse())
            ).block()
        );
    }

    /**
     * Returns a list of all warning headers returned in the response.
     */
    @SuppressWarnings("unchecked")
    public List<String> getWarnings() {
        return ResponseWarningsExtractor.getWarnings(
            publisher.onErrorResume(
                ResponseException.class,
                e -> Mono.just((HttpResponse<Flow.Publisher<List<ByteBuffer>>>) e.getResponse().getHttpResponse())
            ).block()
        );
    }

    /**
     * Returns a list of all headers returned in the response.
     */
    @SuppressWarnings("unchecked")
    public HttpHeaders getHeaders() {
        return publisher.onErrorResume(
            ResponseException.class,
            e -> Mono.just((HttpResponse<Flow.Publisher<List<ByteBuffer>>>) e.getResponse().getHttpResponse())
        ).map(HttpResponse::headers).block();
    }

    /**
     * Returns the value of the first header with a specified name of this message.
     * If there is more than one matching header in the message the first element is returned.
     * If there is no matching header in the message <code>null</code> is returned.
     *
     * @param name header name
     */
    @SuppressWarnings("unchecked")
    public String getHeader(String name) {
        return publisher.onErrorResume(
            ResponseException.class,
            e -> Mono.just((HttpResponse<Flow.Publisher<List<ByteBuffer>>>) e.getResponse().getHttpResponse())
        ).mapNotNull(response -> response.headers().firstValue(name).orElse(null)).block();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;

import java.util.List;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * HTTP Streaming Response from OpenSearch. <strong>This is an experimental API.</strong>
 */
public class StreamingResponse<T> {
    private final RequestLine requestLine;
    private final Mono<Message<HttpResponse, Publisher<T>>> publisher;
    private volatile HttpHost host;

    /**
     * Constructor
     * @param requestLine request line
     * @param publisher message publisher(response with a body)
     */
    public StreamingResponse(RequestLine requestLine, Publisher<Message<HttpResponse, Publisher<T>>> publisher) {
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
    public Publisher<T> getBody() {
        return publisher.flatMapMany(m -> Flux.from(m.getBody()));
    }

    /**
     * Returns the status line of the current response
     */
    public StatusLine getStatusLine() {
        return publisher.map(Message::getHead)
            .onErrorResume(ResponseException.class, e -> Mono.just(e.getResponse().getHttpResponse()))
            .map(HttpResponse::getStatusLine)
            .block();
    }

    /**
     * Returns a list of all warning headers returned in the response.
     */
    public List<String> getWarnings() {
        return ResponseWarningsExtractor.getWarnings(
            publisher.map(Message::getHead)
                .onErrorResume(ResponseException.class, e -> Mono.just(e.getResponse().getHttpResponse()))
                .block()
        );
    }
}

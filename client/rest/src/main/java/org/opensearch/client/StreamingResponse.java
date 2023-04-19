/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.Message;
import org.apache.hc.core5.http.message.RequestLine;
import org.reactivestreams.Publisher;

/**
 * HTTP Streaming Request to OpenSearch.
 */
public class StreamingResponse<T> {
    private final RequestLine requestLine;
    private final Publisher<Message<HttpResponse, Publisher<T>>> publisher;
    private volatile HttpHost host;

    /**
     * Constructor
     * @param requestLine request line
     * @param publisher publisher
     */
    public StreamingResponse(RequestLine requestLine, Publisher<Message<HttpResponse, Publisher<T>>> publisher) {
        this.requestLine = requestLine;
        this.publisher = publisher;
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
        return Mono.from(publisher).flatMapMany(m -> Flux.from(m.getBody()));
    }
}

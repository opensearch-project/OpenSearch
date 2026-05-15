/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.httpclient;

import java.io.InputStream;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Flow;

/**
 * Holds an opensearch response. It wraps the {@link HttpResponse} returned and associates it with
 * its corresponding {@link RequestLine} and {@link HttpHost}.
 * Note: This is an experimental API.
 */
public class Response {
    private final RequestLine requestLine;
    private final HttpHost host;
    private final HttpResponse<?> response;
    private final List<ByteBuffer> body;
    private final boolean compressed;

    private Response(RequestLine requestLine, HttpHost host, HttpResponse<?> response, List<ByteBuffer> body) {
        Objects.requireNonNull(requestLine, "requestLine cannot be null");
        Objects.requireNonNull(host, "host cannot be null");
        Objects.requireNonNull(response, "response cannot be null");
        this.requestLine = requestLine;
        this.host = host;
        this.response = response;
        this.body = body;
        this.compressed = response.headers().firstValue("Content-Encoding").filter("gzip"::equalsIgnoreCase).map(h -> true).orElse(false);
    }

    static Response fromStreaming(RequestLine requestLine, HttpHost host, HttpResponse<Flow.Publisher<List<ByteBuffer>>> response) {
        return new Response(requestLine, host, response, List.of() /* streaming body could be very large */);
    }

    static Response from(RequestLine requestLine, HttpHost host, HttpResponse<List<ByteBuffer>> response) {
        return new Response(requestLine, host, response, response.body());
    }

    /**
     * Returns the request line that generated this response
     */
    public RequestLine getRequestLine() {
        return requestLine;
    }

    /**
     * Returns the node that returned this response
     */
    public HttpHost getHost() {
        return host;
    }

    /**
     * Returns the status line of the current response
     */
    public StatusLine getStatusLine() {
        return new StatusLine(response);
    }

    /**
     * Returns all the response headers
     */
    public HttpHeaders getHeaders() {
        return response.headers();
    }

    /**
     * Returns the value of the first header with a specified name of this message.
     * If there is more than one matching header in the message the first element is returned.
     * If there is no matching header in the message <code>null</code> is returned.
     *
     * @param name header name
     */
    public String getHeader(String name) {
        return response.headers().firstValue(name).orElse(null);
    }

    /**
     * Returns the response body available, null otherwise
     * @see InputStream
     */
    public List<ByteBuffer> getEntity() {
        return (compressed == false) ? body : BodyUtils.decompress(body);
    }

    /**
     * Returns a list of all warning headers returned in the response.
     */
    public List<String> getWarnings() {
        return ResponseWarningsExtractor.getWarnings(response);
    }

    /**
     * Returns true if there is at least one warning header returned in the
     * response.
     */
    public boolean hasWarnings() {
        List<String> warnings = response.headers().allValues("Warning");
        return warnings != null && warnings.size() > 0;
    }

    HttpResponse<?> getHttpResponse() {
        return response;
    }

    /**
     * Convert response to string representation
     */
    @Override
    public String toString() {
        return "Response{" + "requestLine=" + requestLine + ", host=" + host + ", response=" + getStatusLine() + '}';
    }
}

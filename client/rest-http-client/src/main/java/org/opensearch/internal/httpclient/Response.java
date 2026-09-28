/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.internal.httpclient;

import java.io.InputStream;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Flow;

/**
 * Holds an OpenSearch response. It wraps the {@link HttpResponse} returned and associates it with
 * its corresponding {@link RequestLine} and {@link HttpHost}.
 * Note: This is an experimental API.
 */
public sealed interface Response permits CompressedResponse, NonCompressedResponse {
    /**
     * Create response from streaming conversation
     * @param requestLine request line
     * @param host host
     * @param response underlying HTTP response
     * @return new response instance
     */
    static Response fromStreaming(RequestLine requestLine, HttpHost host, HttpResponse<Flow.Publisher<List<ByteBuffer>>> response) {
        return new NonCompressedResponse(requestLine, host, response, List.of() /* streaming body could be very large */);
    }

    /**
     * Create response from non-streaming conversation
     * @param requestLine request line
     * @param host host
     * @param response underlying HTTP response
     * @return new response instance
     */
    static Response from(RequestLine requestLine, HttpHost host, HttpResponse<List<ByteBuffer>> response) {
        final boolean compressed = response.headers()
            .firstValue("Content-Encoding")
            .filter("gzip"::equalsIgnoreCase)
            .map(h -> true)
            .orElse(false);
        if (compressed == false) {
            return new NonCompressedResponse(requestLine, host, response, response.body());
        } else {
            return new CompressedResponse(requestLine, host, response, response.body());
        }
    }

    /**
     * Returns the request line that generated this response
     */
    RequestLine requestLine();

    /**
     * Returns the node that returned this response
     */
    HttpHost host();

    /**
     * Returns the status line of the current response
     */
    default StatusLine statusLine() {
        return new StatusLine(httpResponse());
    }

    /**
     * Returns all the response headers
     */
    default HttpHeaders headers() {
        return httpResponse().headers();
    }

    /**
     * Returns the response body available, null otherwise
     * @see InputStream
     */
    List<ByteBuffer> entity();

    /**
     * Returns the value of the first header with a specified name of this message.
     * If there is more than one matching header in the message the first element is returned.
     * If there is no matching header in the message <code>null</code> is returned.
     *
     * @param name header name
     */
    default String header(String name) {
        return headers().firstValue(name).orElse(null);
    }

    /**
     * Returns a list of all warning headers returned in the response.
     */
    default List<String> warnings() {
        return ResponseWarningsExtractor.getWarnings(httpResponse());
    }

    /**
     * Returns true if there is at least one warning header returned in the
     * response.
     */
    default boolean hasWarnings() {
        List<String> warnings = headers().allValues("Warning");
        return warnings != null && warnings.size() > 0;
    }

    /**
     * Returns underlying HTTP response instance
     * @return
     */
    HttpResponse<?> httpResponse();
}

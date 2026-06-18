/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.internal.httpclient;

import java.io.InputStream;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

final record CompressedResponse(RequestLine requestLine, HttpHost host, HttpResponse<?> httpResponse, List<ByteBuffer> entity)
    implements
        Response {
    CompressedResponse {
        requestLine = Objects.requireNonNull(requestLine, "requestLine cannot be null");
        host = Objects.requireNonNull(host, "host cannot be null");
        httpResponse = Objects.requireNonNull(httpResponse, "response cannot be null");
    }

    /**
     * Returns the response body available, null otherwise
     * @see InputStream
     */
    public List<ByteBuffer> entity() {
        return BodyUtils.decompress(entity);
    }

    /**
     * Convert response to string representation
     */
    @Override
    public String toString() {
        return "Response{" + "requestLine=" + requestLine() + ", host=" + host() + ", response=" + statusLine() + '}';
    }
}

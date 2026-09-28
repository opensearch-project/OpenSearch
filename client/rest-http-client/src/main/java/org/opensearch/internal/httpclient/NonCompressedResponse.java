/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.internal.httpclient;

import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

final record NonCompressedResponse(RequestLine requestLine, HttpHost host, HttpResponse<?> httpResponse, List<ByteBuffer> entity)
    implements
        Response {
    NonCompressedResponse {
        requestLine = Objects.requireNonNull(requestLine, "requestLine cannot be null");
        host = Objects.requireNonNull(host, "host cannot be null");
        httpResponse = Objects.requireNonNull(httpResponse, "response cannot be null");
    }

    /**
     * Convert response to string representation
     */
    @Override
    public String toString() {
        return "Response{" + "requestLine=" + requestLine() + ", host=" + host() + ", response=" + statusLine() + '}';
    }
}

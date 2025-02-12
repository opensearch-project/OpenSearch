/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.http.HttpResponse;
import org.opensearch.transport.reactor.netty4.Netty4Utils;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

class ReactorNetty4HttpResponse extends DefaultFullHttpResponse implements HttpResponse {
    private final HttpHeaders requestHeaders;

    ReactorNetty4HttpResponse(HttpHeaders requestHeaders, HttpVersion version, RestStatus status, BytesReference content) {
        super(version, HttpResponseStatus.valueOf(status.getStatus()), Netty4Utils.toByteBuf(content));
        this.requestHeaders = requestHeaders;
    }

    @Override
    public void addHeader(String name, String value) {
        headers().add(name, value);
    }

    @Override
    public boolean containsHeader(String name) {
        return headers().contains(name);
    }

    public HttpHeaders requestHeaders() {
        return requestHeaders;
    }
}

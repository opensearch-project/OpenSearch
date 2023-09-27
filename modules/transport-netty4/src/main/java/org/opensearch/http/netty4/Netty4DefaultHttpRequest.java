/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.http.HttpRequest;
import org.opensearch.rest.RestRequest;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;

public class Netty4DefaultHttpRequest extends AbstractNetty4HttpRequest implements HttpRequest {

    private final DefaultHttpRequest request;

    public Netty4DefaultHttpRequest(DefaultHttpRequest request) {
        this(request, new HttpHeadersMap(request.headers()), null);
    }

    private Netty4DefaultHttpRequest(DefaultHttpRequest request, HttpHeadersMap headers, Exception inboundException) {
        this.request = request;
        this.headers = headers;
        this.inboundException = inboundException;
    }

    @Override
    public RestRequest.Method method() {
        return getHttpMethod(request);
    }

    @Override
    public String uri() {
        return request.uri();
    }

    @Override
    public BytesReference content() {
        // throw new RuntimeException("Not implemented");
        return BytesArray.EMPTY;
    }

    @Override
    public final Map<String, List<String>> getHeaders() {
        return headers;
    }

    @Override
    public List<String> strictCookies() {
        String cookieString = request.headers().get(HttpHeaderNames.COOKIE);
        if (cookieString != null) {
            Set<Cookie> cookies = ServerCookieDecoder.STRICT.decode(cookieString);
            if (!cookies.isEmpty()) {
                return ServerCookieEncoder.STRICT.encode(cookies);
            }
        }
        return Collections.emptyList();
    }

    @Override
    public HttpVersion protocolVersion() {
        if (request.protocolVersion().equals(io.netty.handler.codec.http.HttpVersion.HTTP_1_0)) {
            return HttpRequest.HttpVersion.HTTP_1_0;
        } else if (request.protocolVersion().equals(io.netty.handler.codec.http.HttpVersion.HTTP_1_1)) {
            return HttpRequest.HttpVersion.HTTP_1_1;
        } else {
            throw new IllegalArgumentException("Unexpected http protocol version: " + request.protocolVersion());
        }
    }

    @Override
    public HttpRequest removeHeader(String header) {
        return null;
    }

    @Override
    public Netty4HttpResponse createResponse(RestStatus status, BytesReference content) {
        return new Netty4HttpResponse(request.headers(), request.protocolVersion(), status, content);
    }

    @Override
    public Exception getInboundException() {
        return inboundException;
    }

    @Override
    public void release() {
        // do nothing
    }

    @Override
    public HttpRequest releaseAndCopy() {
        return this;
    }

    public DefaultHttpRequest nettyRequest() {
        return request;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.http.netty4;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.http.HttpRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.netty4.Netty4Utils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;

public class Netty4HttpRequest extends AbstractNetty4HttpRequest implements HttpRequest {

    private final FullHttpRequest request;
    private final BytesReference content;
    private final AtomicBoolean released;
    private final boolean pooled;

    Netty4HttpRequest(FullHttpRequest request) {
        this(
            request,
            new HttpHeadersMap(request.headers()),
            new AtomicBoolean(false),
            true,
            Netty4Utils.toBytesReference(request.content())
        );
    }

    Netty4HttpRequest(FullHttpRequest request, Exception inboundException) {
        this(
            request,
            new HttpHeadersMap(request.headers()),
            new AtomicBoolean(false),
            true,
            Netty4Utils.toBytesReference(request.content()),
            inboundException
        );
    }

    private Netty4HttpRequest(
        FullHttpRequest request,
        HttpHeadersMap headers,
        AtomicBoolean released,
        boolean pooled,
        BytesReference content
    ) {
        this(request, headers, released, pooled, content, null);
    }

    private Netty4HttpRequest(
        FullHttpRequest request,
        HttpHeadersMap headers,
        AtomicBoolean released,
        boolean pooled,
        BytesReference content,
        Exception inboundException
    ) {
        this.request = request;
        this.headers = headers;
        this.content = content;
        this.pooled = pooled;
        this.released = released;
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
        assert released.get() == false;
        return content;
    }

    @Override
    public void release() {
        if (pooled && released.compareAndSet(false, true)) {
            request.release();
        }
    }

    @Override
    public HttpRequest releaseAndCopy() {
        assert released.get() == false;
        if (pooled == false) {
            return this;
        }
        try {
            final ByteBuf copiedContent = Unpooled.copiedBuffer(request.content());
            return new Netty4HttpRequest(
                new DefaultFullHttpRequest(
                    request.protocolVersion(),
                    request.method(),
                    request.uri(),
                    copiedContent,
                    request.headers(),
                    request.trailingHeaders()
                ),
                headers,
                new AtomicBoolean(false),
                false,
                Netty4Utils.toBytesReference(copiedContent)
            );
        } finally {
            release();
        }
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
        HttpHeaders headersWithoutContentTypeHeader = new DefaultHttpHeaders();
        headersWithoutContentTypeHeader.add(request.headers());
        headersWithoutContentTypeHeader.remove(header);
        HttpHeaders trailingHeaders = new DefaultHttpHeaders();
        trailingHeaders.add(request.trailingHeaders());
        trailingHeaders.remove(header);
        FullHttpRequest requestWithoutHeader = new DefaultFullHttpRequest(
            request.protocolVersion(),
            request.method(),
            request.uri(),
            request.content(),
            headersWithoutContentTypeHeader,
            trailingHeaders
        );
        return new Netty4HttpRequest(requestWithoutHeader, new HttpHeadersMap(requestWithoutHeader.headers()), released, pooled, content);
    }

    @Override
    public Netty4HttpResponse createResponse(RestStatus status, BytesReference content) {
        return new Netty4HttpResponse(request.headers(), request.protocolVersion(), status, content);
    }

    @Override
    public Exception getInboundException() {
        return inboundException;
    }

    public FullHttpRequest nettyRequest() {
        return request;
    }
}

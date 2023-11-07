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
import org.opensearch.http.HttpRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.reactor.netty4.Netty4Utils;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import reactor.netty.http.server.HttpServerRequest;

class ReactorNetty4HttpRequest implements HttpRequest {
    private final String protocol;
    private final HttpMethod method;
    private final String uri;
    private final ByteBuf content;
    private final HttpHeadersMap headers;
    private final AtomicBoolean released;
    private final Exception inboundException;
    private final boolean pooled;

    ReactorNetty4HttpRequest(HttpServerRequest request, ByteBuf content) {
        this(request, new HttpHeadersMap(request.requestHeaders()), new AtomicBoolean(false), true, content);
    }

    ReactorNetty4HttpRequest(HttpServerRequest request, ByteBuf content, Exception inboundException) {
        this(
            request.protocol(),
            request.method(),
            request.uri(),
            new HttpHeadersMap(request.requestHeaders()),
            new AtomicBoolean(false),
            true,
            content,
            inboundException
        );
    }

    private ReactorNetty4HttpRequest(
        HttpServerRequest request,
        HttpHeadersMap headers,
        AtomicBoolean released,
        boolean pooled,
        ByteBuf content
    ) {
        this(request.protocol(), request.method(), request.uri(), headers, released, pooled, content, null);
    }

    private ReactorNetty4HttpRequest(
        String protocol,
        HttpMethod method,
        String uri,
        HttpHeadersMap headers,
        AtomicBoolean released,
        boolean pooled,
        ByteBuf content,
        Exception inboundException
    ) {

        this.protocol = protocol;
        this.method = method;
        this.uri = uri;
        this.headers = headers;
        this.content = content;
        this.pooled = pooled;
        this.released = released;
        this.inboundException = inboundException;
    }

    @Override
    public RestRequest.Method method() {
        return HttpConversionUtil.convertMethod(method);
    }

    @Override
    public String uri() {
        return uri;
    }

    @Override
    public BytesReference content() {
        assert released.get() == false;
        return Netty4Utils.toBytesReference(content);
    }

    @Override
    public void release() {
        if (pooled && released.compareAndSet(false, true)) {
            content.release();
        }
    }

    @Override
    public HttpRequest releaseAndCopy() {
        assert released.get() == false;
        if (pooled == false) {
            return this;
        }
        try {
            final ByteBuf copiedContent = Unpooled.copiedBuffer(content);
            return new ReactorNetty4HttpRequest(protocol, method, uri, headers, new AtomicBoolean(false), false, copiedContent, null);
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
        String cookieString = headers.httpHeaders.get(HttpHeaderNames.COOKIE);
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
        if (protocol.equals(io.netty.handler.codec.http.HttpVersion.HTTP_1_0.toString())) {
            return HttpRequest.HttpVersion.HTTP_1_0;
        } else if (protocol.equals(io.netty.handler.codec.http.HttpVersion.HTTP_1_1.toString())) {
            return HttpRequest.HttpVersion.HTTP_1_1;
        } else {
            throw new IllegalArgumentException("Unexpected http protocol version: " + protocol);
        }
    }

    @Override
    public HttpRequest removeHeader(String header) {
        HttpHeaders headersWithoutContentTypeHeader = new DefaultHttpHeaders();
        headersWithoutContentTypeHeader.add(headers.httpHeaders);
        headersWithoutContentTypeHeader.remove(header);

        return new ReactorNetty4HttpRequest(
            protocol,
            method,
            uri,
            new HttpHeadersMap(headersWithoutContentTypeHeader),
            released,
            pooled,
            content,
            null
        );
    }

    @Override
    public ReactorNetty4HttpResponse createResponse(RestStatus status, BytesReference content) {
        return new ReactorNetty4HttpResponse(
            headers.httpHeaders,
            io.netty.handler.codec.http.HttpVersion.valueOf(protocol),
            status,
            content
        );
    }

    @Override
    public Exception getInboundException() {
        return inboundException;
    }

    /**
     * A wrapper of {@link HttpHeaders} that implements a map to prevent copying unnecessarily. This class does not support modifications
     * and due to the underlying implementation, it performs case insensitive lookups of key to values.
     *
     * It is important to note that this implementation does have some downsides in that each invocation of the
     * {@link #values()} and {@link #entrySet()} methods will perform a copy of the values in the HttpHeaders rather than returning a
     * view of the underlying values.
     */
    private static class HttpHeadersMap implements Map<String, List<String>> {

        private final HttpHeaders httpHeaders;

        private HttpHeadersMap(HttpHeaders httpHeaders) {
            this.httpHeaders = httpHeaders;
        }

        @Override
        public int size() {
            return httpHeaders.size();
        }

        @Override
        public boolean isEmpty() {
            return httpHeaders.isEmpty();
        }

        @Override
        public boolean containsKey(Object key) {
            return key instanceof String && httpHeaders.contains((String) key);
        }

        @Override
        public boolean containsValue(Object value) {
            return value instanceof List && httpHeaders.names().stream().map(httpHeaders::getAll).anyMatch(value::equals);
        }

        @Override
        public List<String> get(Object key) {
            return key instanceof String ? httpHeaders.getAll((String) key) : null;
        }

        @Override
        public List<String> put(String key, List<String> value) {
            throw new UnsupportedOperationException("modifications are not supported");
        }

        @Override
        public List<String> remove(Object key) {
            throw new UnsupportedOperationException("modifications are not supported");
        }

        @Override
        public void putAll(Map<? extends String, ? extends List<String>> m) {
            throw new UnsupportedOperationException("modifications are not supported");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("modifications are not supported");
        }

        @Override
        public Set<String> keySet() {
            return httpHeaders.names();
        }

        @Override
        public Collection<List<String>> values() {
            return httpHeaders.names().stream().map(k -> Collections.unmodifiableList(httpHeaders.getAll(k))).collect(Collectors.toList());
        }

        @Override
        public Set<Entry<String, List<String>>> entrySet() {
            return httpHeaders.names()
                .stream()
                .map(k -> new AbstractMap.SimpleImmutableEntry<>(k, httpHeaders.getAll(k)))
                .collect(Collectors.toSet());
        }
    }
}

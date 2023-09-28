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
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

public class Netty4DefaultHttpRequestTests extends OpenSearchTestCase {
    public void testDefaultHttpRequestHasEmptyContent() throws Exception {
        HttpHeaders testHeaders = new DefaultHttpHeaders();
        testHeaders.add("test-header", "test-value");
        DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "https://localhost:9200");
        Netty4DefaultHttpRequest netty4Request = new Netty4DefaultHttpRequest(request);

        assertEquals("Expected content to be empty", BytesArray.EMPTY, netty4Request.content());
    }

    public void testDefaultHttpRequestAndFullHttpRequestHaveSameStructure() throws Exception {
        HttpHeaders testHeaders = new DefaultHttpHeaders();
        testHeaders.add("test-header", "test-value");
        DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "https://localhost:9200", testHeaders);
        Netty4DefaultHttpRequest netty4Request = new Netty4DefaultHttpRequest(request);

        ByteBuf expectedByteBuf = Unpooled.copiedBuffer("Hello, World!".getBytes(StandardCharsets.UTF_8));
        BytesReference expectedBytesArray = new BytesArray("Hello, World!".getBytes(StandardCharsets.UTF_8));

        HttpHeaders trailingHeaders = new DefaultHttpHeaders();
        FullHttpRequest fullRequest = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1,
            HttpMethod.GET,
            "https://localhost:9200",
            expectedByteBuf,
            testHeaders,
            trailingHeaders
        );
        Netty4HttpRequest fullNetty4Request = new Netty4HttpRequest(fullRequest);

        assertEquals("Expected methods to be equal", netty4Request.method(), fullNetty4Request.method());
        assertEquals("Expected URIs to be equal", netty4Request.uri(), fullNetty4Request.uri());
        assertEquals("Expected Protocol Versions to be equal", netty4Request.protocolVersion(), fullNetty4Request.protocolVersion());

        assertEquals("Expected headers to be of equal length", netty4Request.headers.size(), fullNetty4Request.headers.size());
        for (String header : netty4Request.headers.keySet()) {
            assertTrue(fullNetty4Request.headers.containsKey(header));
            List<String> headerValue = netty4Request.headers.get(header).stream().sorted().collect(Collectors.toList());
            List<String> otherValue = fullNetty4Request.headers.get(header).stream().sorted().collect(Collectors.toList());
            assertEquals(headerValue, otherValue);
        }

        assertEquals("Expected content to be empty", BytesArray.EMPTY, netty4Request.content());
        assertEquals("Expected content to not be empty", expectedBytesArray, fullNetty4Request.content());
    }
}

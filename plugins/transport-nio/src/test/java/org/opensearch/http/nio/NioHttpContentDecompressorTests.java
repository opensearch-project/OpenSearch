/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.nio;

import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;

public class NioHttpContentDecompressorTests extends OpenSearchTestCase {

    private static FullHttpRequest requestWithBody(final CharSequence contentEncoding, final byte[] body) {
        final FullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1,
            HttpMethod.POST,
            "/_bulk",
            Unpooled.wrappedBuffer(body)
        );
        request.headers().set(HttpHeaderNames.CONTENT_ENCODING, contentEncoding);
        request.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.length);
        return request;
    }

    public void testZstdContentIsPassedThroughUndecoded() {
        final EmbeddedChannel channel = new EmbeddedChannel(new NioHttpContentDecompressor());
        final byte[] body = randomByteArrayOfLength(randomIntBetween(1, 128));

        assertTrue(channel.writeInbound(requestWithBody(HttpHeaderValues.ZSTD, body)));

        final FullHttpRequest decoded = channel.readInbound();
        try {
            assertEquals(HttpHeaderValues.ZSTD.toString(), decoded.headers().get(HttpHeaderNames.CONTENT_ENCODING));
            assertArrayEquals(body, ByteBufUtil.getBytes(decoded.content()));
        } finally {
            decoded.release();
        }
        assertFalse(channel.finish());
    }

    public void testGzipContentIsStillDecompressed() throws IOException {
        final EmbeddedChannel channel = new EmbeddedChannel(new NioHttpContentDecompressor());
        final byte[] body = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);
        final ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
            gzip.write(body);
        }

        assertTrue(channel.writeInbound(requestWithBody(HttpHeaderValues.GZIP, compressed.toByteArray())));

        // on successful decode the decompressor re-emits the request headers followed by decoded content chunks
        final HttpRequest decodedRequest = channel.readInbound();
        assertNotEquals(HttpHeaderValues.GZIP.toString(), decodedRequest.headers().get(HttpHeaderNames.CONTENT_ENCODING));
        final ByteArrayOutputStream decoded = new ByteArrayOutputStream();
        Object msg;
        while ((msg = channel.readInbound()) != null) {
            final HttpContent content = (HttpContent) msg;
            decoded.write(ByteBufUtil.getBytes(content.content()));
            content.release();
        }
        assertArrayEquals(body, decoded.toByteArray());
        assertFalse(channel.finish());
    }
}

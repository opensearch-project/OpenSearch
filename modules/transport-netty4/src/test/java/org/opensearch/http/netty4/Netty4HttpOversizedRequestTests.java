/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.SharedGroupFactory;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.util.ReferenceCountUtil;

import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_READ_TIMEOUT;
import static org.opensearch.http.TestDispatcherBuilder.dispatcherBuilderWithDefaults;
import static org.opensearch.http.netty4.Netty4HttpRequestSizeTestClient.assertResponse;
import static org.opensearch.http.netty4.Netty4HttpRequestSizeTestClient.bulkBody;
import static org.opensearch.http.netty4.Netty4HttpRequestSizeTestClient.connect;
import static org.opensearch.http.netty4.Netty4HttpRequestSizeTestClient.encode;
import static org.opensearch.http.netty4.Netty4HttpRequestSizeTestClient.encodeStored;
import static org.opensearch.http.netty4.Netty4HttpRequestSizeTestClient.headers;
import static org.opensearch.http.netty4.Netty4HttpRequestSizeTestClient.request;
import static org.opensearch.http.netty4.Netty4HttpRequestSizeTestClient.send;

/**
 * Exercises HTTP decoding, decompression, aggregation and socket closure over real TCP connections.
 */
public class Netty4HttpOversizedRequestTests extends OpenSearchTestCase {
    private static final int LIMIT = 1024;
    private ThreadPool threadPool;
    private MockBigArrays bigArrays;
    private final AtomicInteger dispatched = new AtomicInteger();
    private final AtomicInteger dispatchedPosts = new AtomicInteger();

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    @After
    public void shutdown() {
        threadPool.shutdownNow();
    }

    public void testOversizedRequestsReturn413BeforeEof() throws Exception {
        try (Netty4HttpServerTransport transport = transport(null)) {
            transport.start();
            for (String encoding : new String[] { "identity", "gzip", "deflate" }) {
                for (boolean chunked : new boolean[] { false, true }) {
                    for (int fragmentSize : new int[] { 1, 17, 1024, Integer.MAX_VALUE }) {
                        byte[] body = encode(bulkBody(LIMIT + 1), encoding);
                        if (encoding.equals("identity") == false) {
                            assertTrue("The decompressed limit must be exceeded while the wire size fits", body.length < LIMIT);
                        }
                        String scenario = encoding + ", chunked=" + chunked + ", fragmentSize=" + fragmentSize;
                        try (Socket socket = connect(transport)) {
                            send(socket.getOutputStream(), request(body, encoding, chunked, true), fragmentSize);
                            assertResponse(socket.getInputStream(), 413, scenario);
                            assertEquals("Complete rejection must precede EOF: " + scenario, -1, socket.getInputStream().read());
                        }
                    }
                }
            }
            assertEquals("Oversized requests must never reach REST dispatch", 0, dispatched.get());
        }
    }

    public void testRequestsAtLimitAreAccepted() throws Exception {
        try (Netty4HttpServerTransport transport = transport(null)) {
            transport.start();
            for (String encoding : new String[] { "identity", "gzip", "deflate" }) {
                for (boolean chunked : new boolean[] { false, true }) {
                    try (Socket socket = connect(transport)) {
                        send(socket.getOutputStream(), request(encode(bulkBody(LIMIT), encoding), encoding, chunked, true), 17);
                        assertResponse(socket.getInputStream(), 200, encoding + ", chunked=" + chunked);
                        assertEquals(-1, socket.getInputStream().read());
                    }
                }
            }
            assertEquals(6, dispatched.get());
        }
    }

    public void testEncodedSizeBoundary() throws Exception {
        try (Netty4HttpServerTransport transport = transport(null)) {
            transport.start();
            int accepted = 0;
            for (String encoding : new String[] { "gzip", "deflate" }) {
                int overhead = encodeStored(new byte[1], encoding).length - 1;
                for (int size : new int[] { LIMIT - 1, LIMIT, LIMIT + 1 }) {
                    byte[] decoded = bulkBody(size - overhead);
                    byte[] body = encodeStored(decoded, encoding);
                    assertEquals(size, body.length);
                    assertTrue("The decoded body must fit even when encoded content exceeds the limit", decoded.length < LIMIT);
                    for (boolean chunked : new boolean[] { false, true }) {
                        for (int fragmentSize : new int[] { 1, 17, Integer.MAX_VALUE }) {
                            try (Socket socket = connect(transport)) {
                                send(socket.getOutputStream(), request(body, encoding, chunked, true), fragmentSize);
                                assertResponse(socket.getInputStream(), size > LIMIT ? 413 : 200, encoding + ", encoded size=" + size);
                                assertEquals(-1, socket.getInputStream().read());
                            }
                            if (size <= LIMIT) {
                                accepted++;
                            }
                        }
                    }
                }
            }
            assertEquals("Only bodies within both limits may reach dispatch", accepted, dispatched.get());
        }
    }

    public void testEncodedContentLengthRejectsBeforeBodyOrContinue() throws Exception {
        try (Netty4HttpServerTransport transport = transport(null)) {
            transport.start();
            for (String encoding : new String[] { "gzip", "deflate" }) {
                for (boolean expect : new boolean[] { false, true }) {
                    try (Socket socket = connect(transport)) {
                        String headers = new String(headers(LIMIT + 1, encoding, false, false), StandardCharsets.US_ASCII);
                        if (expect) {
                            headers = headers.replace("\r\n\r\n", "\r\nExpect: 100-continue\r\n\r\n");
                        }
                        socket.getOutputStream().write(headers.getBytes(StandardCharsets.US_ASCII));
                        socket.getOutputStream().flush();
                        assertResponse(socket.getInputStream(), 413, "encoded Content-Length, expect=" + expect);
                        socket.shutdownOutput();
                        assertEquals(-1, socket.getInputStream().read());
                    }
                }
            }
            assertEquals(0, dispatched.get());
        }
    }

    public void testEncodedSizeResetsBetweenRequests() throws Exception {
        try (Netty4HttpServerTransport transport = transport(null)) {
            transport.start();
            try (Socket socket = connect(transport)) {
                for (String encoding : new String[] { "gzip", "deflate" }) {
                    int overhead = encodeStored(new byte[1], encoding).length - 1;
                    byte[] body = encodeStored(bulkBody(LIMIT - overhead), encoding);
                    assertEquals(LIMIT, body.length);
                    // Chunk framing takes the wire message over the limit, but is not part of the body.
                    send(socket.getOutputStream(), request(body, encoding, true, false, 1), 17);
                    assertResponse(socket.getInputStream(), 200, "successive encoded body at the limit");
                }
            }
            assertEquals(2, dispatched.get());
        }
    }

    public void testHttp2EncodedAndDecodedLimits() throws Exception {
        try (Netty4HttpServerTransport transport = transport(null); Netty4HttpClient client = Netty4HttpClient.http2()) {
            transport.start();
            List<HttpRequest> requests = new ArrayList<>();
            for (String encoding : new String[] { "gzip", "deflate" }) {
                for (boolean contentLength : new boolean[] { false, true }) {
                    for (byte[] body : new byte[][] {
                        encodeStored(bulkBody(LIMIT), encoding),
                        encode(bulkBody(LIMIT + 1), encoding),
                        encode(bulkBody(LIMIT), encoding) }) {
                        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                            HttpVersion.HTTP_1_1,
                            HttpMethod.POST,
                            "/",
                            Unpooled.wrappedBuffer(body)
                        );
                        request.headers().set(HttpHeaderNames.HOST, "localhost");
                        request.headers().set(HttpConversionUtil.ExtensionHeaderNames.SCHEME.text(), "http");
                        request.headers().set(HttpHeaderNames.CONTENT_ENCODING, encoding);
                        if (contentLength) {
                            request.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, body.length);
                        }
                        requests.add(request);
                    }
                }
            }
            List<FullHttpResponse> responses = client.sendRequests(transport.boundAddress().boundAddresses()[0].address(), requests);
            try {
                assertEquals(8, responses.stream().filter(response -> response.status().code() == 413).count());
                assertEquals(
                    "Other streams on the same connection must remain usable",
                    4,
                    responses.stream().filter(response -> response.status().code() == 200).count()
                );
            } finally {
                responses.forEach(ReferenceCountUtil::release);
            }
            // The client also performs a valid HTTP/1 upgrade GET before sending the HTTP/2 POSTs.
            assertEquals("Only accepted POSTs may reach dispatch", 4, dispatchedPosts.get());
        }
    }

    public void testEncodedLimitRejectsBeforeGzipProducesAnyOutput() throws Exception {
        try (Netty4HttpServerTransport transport = transport(null)) {
            transport.start();
            try (Socket socket = connect(transport)) {
                byte[] gzip = encode(new byte[0], "gzip");
                gzip[3] = 4; // FEXTRA: the gzip header includes a length-prefixed optional field.
                ByteArrayOutputStream prefix = new ByteArrayOutputStream();
                prefix.write(gzip, 0, 10);
                prefix.write(0);
                prefix.write(8); // 2048 extra bytes, encoded little-endian.
                prefix.write(new byte[LIMIT + 1 - prefix.size()]);
                socket.getOutputStream().write(headers(0, "gzip", true, false));
                socket.getOutputStream().write("1000\r\n".getBytes(StandardCharsets.US_ASCII));
                // Neither the chunk nor the gzip header is complete; no decompressed bytes are available.
                socket.getOutputStream().write(prefix.toByteArray());
                socket.getOutputStream().flush();
                assertResponse(socket.getInputStream(), 413, "oversized gzip header with no decoded output");
                socket.shutdownOutput();
                assertEquals(-1, socket.getInputStream().read());
            }
            assertEquals(0, dispatched.get());
        }
    }

    public void testHttp2StopsIncompleteUploadWithNoErrorAfter413() throws Exception {
        try (Netty4HttpServerTransport transport = transport(null)) {
            transport.start();
            try (Socket socket = connect(transport)) {
                DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                output.write("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
                output.write(new byte[] { 0, 0, 0, 4, 0, 0, 0, 0, 0 }); // Empty SETTINGS frame.
                ByteBuf headers = Unpooled.buffer();
                try {
                    new DefaultHttp2HeadersEncoder().encodeHeaders(
                        1,
                        new DefaultHttp2Headers().method("POST")
                            .scheme("http")
                            .authority("localhost")
                            .path("/")
                            .add("content-encoding", "gzip")
                            .add("content-length", Integer.toString(LIMIT + 1)),
                        headers
                    );
                    output.writeInt((headers.readableBytes() << 8) | 1); // 24-bit length and HEADERS frame type.
                    output.writeByte(4); // END_HEADERS, but not END_STREAM: the upload remains open.
                    output.writeInt(1);
                    headers.readBytes(output, headers.readableBytes());
                } finally {
                    headers.release();
                }
                output.flush();

                DataInputStream input = new DataInputStream(socket.getInputStream());
                boolean responseComplete = false;
                boolean resetReceived = false;
                DefaultHttp2HeadersDecoder decoder = new DefaultHttp2HeadersDecoder();
                for (int frames = 0; frames < 10 && resetReceived == false; frames++) {
                    int lengthAndType = input.readInt();
                    int length = lengthAndType >>> 8;
                    int type = lengthAndType & 255;
                    int flags = input.readUnsignedByte();
                    int stream = input.readInt() & Integer.MAX_VALUE;
                    assertTrue("Unexpectedly large HTTP/2 frame", length <= 16384);
                    byte[] bytes = input.readNBytes(length);
                    assertEquals(length, bytes.length);
                    ByteBuf payload = Unpooled.wrappedBuffer(bytes);
                    try {
                        if (type == 1 && stream == 1) {
                            assertEquals("413", decoder.decodeHeaders(stream, payload).status().toString());
                            responseComplete = (flags & 1) != 0;
                        } else if (type == 3 && stream == 1) {
                            assertTrue("The complete response must precede RST_STREAM", responseComplete);
                            assertEquals("NO_ERROR preserves the response while stopping the upload", 0, payload.readUnsignedInt());
                            resetReceived = true;
                        }
                    } finally {
                        payload.release();
                    }
                }
                assertTrue("The server must stop the unfinished upload", resetReceived);
            }
            assertEquals(0, dispatched.get());
        }
    }

    public void testContinuedUploadAfterRejectionDoesNotResetResponse() throws Exception {
        try (Netty4HttpServerTransport transport = transport(null)) {
            transport.start();
            for (String encoding : new String[] { "identity", "gzip", "deflate" }) {
                ByteArrayOutputStream encoded = new ByteArrayOutputStream();
                byte[] body = bulkBody(32 * LIMIT);
                int split;
                // Sync-flush the prefix so the decompressed limit is crossed before the remainder is sent.
                try (OutputStream compressor = switch (encoding) {
                    case "gzip" -> new GZIPOutputStream(encoded, 8192, true);
                    case "deflate" -> new DeflaterOutputStream(encoded, new Deflater(), 8192, true);
                    default -> encoded;
                }) {
                    compressor.write(body, 0, LIMIT + 1);
                    compressor.flush();
                    split = encoded.size();
                    compressor.write(body, LIMIT + 1, body.length - LIMIT - 1);
                }
                byte[] wireBody = encoded.toByteArray();
                for (boolean chunked : new boolean[] { false, true }) {
                    try (Socket socket = connect(transport)) {
                        OutputStream output = socket.getOutputStream();
                        output.write(headers(wireBody.length, encoding, chunked, true));
                        if (chunked) {
                            output.write((Integer.toHexString(wireBody.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
                        }
                        output.write(wireBody, 0, split);
                        output.flush();
                        // Leave the response unread until the upload finishes, as a client blocked in write() might.
                        assertBusy(() -> assertTrue(socket.getInputStream().available() > 0));
                        output.write(wireBody, split, wireBody.length - split);
                        if (chunked) {
                            output.write("\r\n0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
                        }
                        output.flush();
                        socket.shutdownOutput();
                        assertResponse(socket.getInputStream(), 413, encoding + ", chunked=" + chunked);
                        assertEquals(-1, socket.getInputStream().read());
                    }
                }
            }
            assertEquals(0, dispatched.get());
        }
    }

    public void testDecompressionFailureCannotOvertakePending413() throws Exception {
        assertGzipTrailerCannotOvertakePending413(true);
    }

    public void testValidGzipTrailerCannotOvertakePending413() throws Exception {
        assertGzipTrailerCannotOvertakePending413(false);
    }

    public void testPausedCompressedUploadReturns413BeforeReadTimeoutClosesConnection() throws Exception {
        RejectionWriteGate gate = new RejectionWriteGate();
        try (Netty4HttpServerTransport transport = transport(gate, true)) {
            transport.start();
            try (Socket socket = connect(transport)) {
                byte[] body = encode(bulkBody(32 * LIMIT), "gzip");
                socket.getOutputStream().write(headers(body.length, "gzip", false, false));
                socket.getOutputStream().write(body, 0, body.length - 8);
                socket.getOutputStream().flush();
                assertTrue(
                    "The payload must exceed the decompressed limit before its trailer",
                    gate.rejectionPending.await(10, TimeUnit.SECONDS)
                );
                // Leave the valid gzip trailer unsent and wait for the actual ReadTimeoutHandler to fire.
                // Holding the real write promise gives a deterministic timeout / response ordering without sleeps.
                assertTrue("The paused upload must trigger a real read timeout", gate.timeoutObserved.await(10, TimeUnit.SECONDS));
                gate.release();
                assertResponse(socket.getInputStream(), 413, "valid gzip upload paused before its trailer");
                assertEquals(-1, socket.getInputStream().read());
            } finally {
                gate.release();
            }
            assertEquals(0, dispatched.get());
        }
    }

    public void testClientFinCannotOvertakePending413() throws Exception {
        RejectionWriteGate gate = new RejectionWriteGate();
        try (Netty4HttpServerTransport transport = transport(gate)) {
            transport.start();
            try (Socket socket = connect(transport)) {
                socket.getOutputStream().write(request(encode(bulkBody(32 * LIMIT), "gzip"), "gzip", false, false));
                socket.getOutputStream().flush();
                assertTrue(gate.rejectionPending.await(10, TimeUnit.SECONDS));
                // A client may finish uploading with FIN while keeping its input open for the response.
                socket.shutdownOutput();
                assertTrue("The client's FIN must have been processed", gate.inputClosed.await(10, TimeUnit.SECONDS));
                gate.release();
                assertResponse(socket.getInputStream(), 413, "client half-closes after a complete gzip upload");
                assertEquals(-1, socket.getInputStream().read());
            } finally {
                gate.release();
            }
            assertEquals(0, dispatched.get());
        }
    }

    public void testClientFinCannotOvertakeEncodedSizeRejection() throws Exception {
        RejectionWriteGate gate = new RejectionWriteGate();
        try (Netty4HttpServerTransport transport = transport(gate)) {
            transport.start();
            try (Socket socket = connect(transport)) {
                socket.getOutputStream().write(headers(LIMIT + 1, "gzip", false, false));
                socket.getOutputStream().flush();
                assertTrue(gate.rejectionPending.await(10, TimeUnit.SECONDS));
                socket.shutdownOutput();
                assertTrue(gate.inputClosed.await(10, TimeUnit.SECONDS));
                gate.release();
                assertResponse(socket.getInputStream(), 413, "client FIN before compressed body and pending 413");
                assertEquals(-1, socket.getInputStream().read());
            } finally {
                gate.release();
            }
            assertEquals(0, dispatched.get());
        }
    }

    private void assertGzipTrailerCannotOvertakePending413(boolean invalidChecksum) throws Exception {
        RejectionWriteGate gate = new RejectionWriteGate();
        try (Netty4HttpServerTransport transport = transport(gate)) {
            transport.start();
            try (Socket socket = connect(transport)) {
                byte[] body = encode(bulkBody(32 * LIMIT), "gzip");
                // Separate the gzip trailer from the payload so oversize detection happens first.
                byte[] headers = headers(body.length, "gzip", false, true);
                socket.getOutputStream().write(headers);
                socket.getOutputStream().write(body, 0, body.length - 8);
                socket.getOutputStream().flush();
                assertTrue("413 must have been generated", gate.rejectionPending.await(10, TimeUnit.SECONDS));

                // An invalid CRC produces a real decompressor exception while the response write is pending.
                // The transport must preserve its already chosen response even when more input fails decoding.
                if (invalidChecksum) {
                    body[body.length - 8] ^= 1;
                }
                socket.getOutputStream().write(body, body.length - 8, 8);
                socket.getOutputStream().flush();
                assertTrue("The gzip trailer bytes must have been processed", gate.nextRead.await(10, TimeUnit.SECONDS));
                gate.release();
                assertResponse(socket.getInputStream(), 413, "gzip trailer with a pending rejection, invalidChecksum=" + invalidChecksum);
                assertEquals(-1, socket.getInputStream().read());
            } finally {
                gate.release();
            }
            assertEquals(0, dispatched.get());
        }
    }

    public void testReadTimeoutCannotOvertakePending413() throws Exception {
        assertReadTimeoutCannotOvertakePending413(true, false);
    }

    public void testReadTimeoutCannotOvertakeRecoverable413() throws Exception {
        assertReadTimeoutCannotOvertakePending413(false, false);
    }

    public void testReadTimeoutCannotOvertakeExpectContinue413() throws Exception {
        assertReadTimeoutCannotOvertakePending413(false, true);
        assertReadTimeoutCannotOvertakePending413(true, true);
    }

    private void assertReadTimeoutCannotOvertakePending413(boolean close, boolean expectContinue) throws Exception {
        RejectionWriteGate gate = new RejectionWriteGate();
        try (Netty4HttpServerTransport transport = transport(gate)) {
            transport.start();
            try (Socket socket = connect(transport)) {
                byte[] requestHeaders = headers(LIMIT + 1, "identity", false, close);
                if (expectContinue) {
                    requestHeaders = new String(requestHeaders, StandardCharsets.US_ASCII).replace(
                        "\r\n\r\n",
                        "\r\nExpect: 100-continue\r\n\r\n"
                    ).getBytes(StandardCharsets.US_ASCII);
                }
                socket.getOutputStream().write(requestHeaders);
                socket.getOutputStream().flush();
                assertTrue(gate.rejectionPending.await(10, TimeUnit.SECONDS));
                // Deliver the timeout on the real event loop while the actual response write is pending.
                gate.context.executor().submit(() -> gate.context.pipeline().fireExceptionCaught(ReadTimeoutException.INSTANCE)).sync();
                assertTrue("A read timeout must not close ahead of the pending rejection", gate.context.channel().isOpen());
                assertFalse("The rejection write must remain pending", gate.promise.isDone());
                gate.release();
                assertResponse(socket.getInputStream(), 413, "read timeout with a pending rejection");
                assertEquals(-1, socket.getInputStream().read());
            } finally {
                gate.release();
            }
            assertEquals(0, dispatched.get());
        }
    }

    public void testIncompleteOversizedUploadIsClosedAfterDrainTimeout() throws Exception {
        try (Netty4HttpServerTransport transport = transport(null)) {
            transport.start();
            try (Socket socket = connect(transport)) {
                socket.getOutputStream().write(headers(LIMIT + 1, "identity", false, true));
                socket.getOutputStream().flush();
                assertResponse(socket.getInputStream(), 413, "sender never finishes the upload");
                assertEquals(-1, socket.getInputStream().read());
                // Keep the client's input side open: the drain timeout must release the accepted server channel.
                assertBusy(() -> assertEquals(0, transport.stats().getServerOpen()), 10, TimeUnit.SECONDS);
            }
        }
    }

    public void testRecoverableRejectionAllowsNextRequest() throws Exception {
        try (Netty4HttpServerTransport transport = transport(null)) {
            transport.start();
            try (Socket socket = connect(transport)) {
                send(socket.getOutputStream(), request(bulkBody(LIMIT + 1), "identity", false, false), 17);
                assertResponse(socket.getInputStream(), 413, "recoverable content-length rejection");
                send(socket.getOutputStream(), request(bulkBody(LIMIT), "identity", false, true), 17);
                assertResponse(socket.getInputStream(), 200, "valid request after recoverable rejection");
                assertEquals(-1, socket.getInputStream().read());
            }
            assertEquals(1, dispatched.get());
        }
    }

    public void testExpectContinueRejectionAllowsNextRequest() throws Exception {
        try (Netty4HttpServerTransport transport = transport(null)) {
            transport.start();
            try (Socket socket = connect(transport)) {
                String requestHeaders = new String(headers(LIMIT + 1, "identity", false, false), StandardCharsets.US_ASCII);
                socket.getOutputStream()
                    .write(requestHeaders.replace("\r\n\r\n", "\r\nExpect: 100-continue\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
                socket.getOutputStream().flush();
                assertResponse(socket.getInputStream(), 413, "recoverable expectation rejection");
                send(socket.getOutputStream(), request(bulkBody(LIMIT), "identity", false, true), 17);
                assertResponse(socket.getInputStream(), 200, "valid request after expectation rejection");
                assertEquals(-1, socket.getInputStream().read());
            }
            assertEquals(1, dispatched.get());
        }
    }

    private Netty4HttpServerTransport transport(RejectionWriteGate gate) {
        return transport(gate, false);
    }

    private Netty4HttpServerTransport transport(RejectionWriteGate gate, boolean readTimeout) {
        Settings.Builder builder = Settings.builder()
            .put("http.port", "0")
            .put("network.host", "127.0.0.1")
            .put(SETTING_HTTP_MAX_CONTENT_LENGTH.getKey(), LIMIT + "b");
        if (readTimeout) {
            builder.put(SETTING_HTTP_READ_TIMEOUT.getKey(), "100ms");
        }
        Settings settings = builder.build();
        HttpServerTransport.Dispatcher dispatcher = dispatcherBuilderWithDefaults().withDispatchRequest((request, channel, context) -> {
            dispatched.incrementAndGet();
            if (request.method() == org.opensearch.rest.RestRequest.Method.POST) {
                dispatchedPosts.incrementAndGet();
            }
            channel.sendResponse(new org.opensearch.rest.BytesRestResponse(OK, "text/plain", new BytesArray("done")));
        }).build();
        return new Netty4HttpServerTransport(
            settings,
            new NetworkService(Collections.emptyList()),
            bigArrays,
            threadPool,
            xContentRegistry(),
            dispatcher,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            new SharedGroupFactory(settings),
            NoopTracer.INSTANCE
        ) {
            @Override
            public void onException(HttpChannel channel, Exception cause) {
                super.onException(channel, cause);
                if (gate != null && cause instanceof ReadTimeoutException) {
                    gate.timeoutObserved.countDown();
                }
            }

            @Override
            public ChannelHandler configureServerChannelHandler() {
                return new HttpChannelHandler(this, handlingSettings) {
                    @Override
                    protected void initChannel(Channel channel) throws Exception {
                        super.initChannel(channel);
                        if (gate != null) {
                            channel.pipeline().addFirst("rejection_write_gate", gate);
                        }
                    }
                };
            }
        };
    }

    /**
     * Holds the encoded rejection and its real write promise, modeling a busy socket without sleeping or filling OS buffers.
     */
    private static class RejectionWriteGate extends ChannelDuplexHandler {
        private final CountDownLatch rejectionPending = new CountDownLatch(1);
        private final CountDownLatch nextRead = new CountDownLatch(1);
        private final CountDownLatch timeoutObserved = new CountDownLatch(1);
        private final CountDownLatch inputClosed = new CountDownLatch(1);
        private volatile ChannelHandlerContext context;
        private Object message;
        private ChannelPromise promise;
        private int bytesReadAfterRejection;

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise writePromise) throws Exception {
            if (msg instanceof ByteBuf buffer && buffer.toString(StandardCharsets.US_ASCII).startsWith("HTTP/1.1 413 ")) {
                context = ctx;
                message = msg;
                promise = writePromise;
                ctx.pipeline().addFirst("rejection_read_observer", new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext readContext, Object input) throws Exception {
                        int bytes = input instanceof ByteBuf buffer ? buffer.readableBytes() : 0;
                        super.channelRead(readContext, input);
                        bytesReadAfterRejection += bytes;
                        if (bytesReadAfterRejection >= 8) {
                            nextRead.countDown();
                        }
                    }
                });
                rejectionPending.countDown();
            } else {
                ctx.write(msg, writePromise);
            }
        }

        void release() throws Exception {
            ChannelHandlerContext ctx = context;
            if (ctx != null) {
                ctx.executor().submit(() -> {
                    if (message != null) {
                        Object pending = message;
                        message = null;
                        ctx.writeAndFlush(pending, promise);
                    }
                }).sync();
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
            super.userEventTriggered(ctx, event);
            if (event instanceof ChannelInputShutdownEvent) {
                inputClosed.countDown();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
            inputClosed.countDown();
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) {
            if (message != null) {
                ReferenceCountUtil.release(message);
                message = null;
                promise.tryFailure(new ClosedChannelException());
            }
        }
    }
}

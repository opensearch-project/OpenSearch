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
import java.io.OutputStream;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.ChannelInputShutdownEvent;
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

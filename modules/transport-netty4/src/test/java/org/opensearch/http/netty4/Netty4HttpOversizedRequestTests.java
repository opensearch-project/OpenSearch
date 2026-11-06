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
import org.opensearch.core.rest.RestStatus;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.HttpTransportSettings;
import org.opensearch.http.netty4.Netty4HttpRawClient.Encoding;
import org.opensearch.http.netty4.Netty4HttpRawClient.RequestBuilder;
import org.opensearch.http.netty4.Netty4HttpRawClient.Response;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.SharedGroupFactory;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
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
import io.netty.channel.socket.DuplexChannel;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.util.ReferenceCountUtil;

import static org.opensearch.http.TestDispatcherBuilder.dispatcherBuilderWithDefaults;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;

/**
 * Checks that a request larger than {@code http.max_content_length} is answered with a {@code 413} the client can read,
 * whatever the encoding, framing and timing, and that the connection is then either reused or closed without a reset.
 * <p>
 * The tests drive a real transport over TCP with {@link Netty4HttpRawClient}. Races are made deterministic with
 * {@link ResponseHold}, which holds the encoded {@code 413} at the edge of the pipeline until the test has made the
 * competing event (client FIN, read timeout, decoder failure) happen on the server.
 */
public class Netty4HttpOversizedRequestTests extends OpenSearchTestCase {

    private static final int LIMIT = 64 * 1024;

    private NetworkService networkService;
    private MockBigArrays bigArrays;
    private ThreadPool threadPool;
    private final AtomicInteger dispatched = new AtomicInteger();

    @Before
    public void setup() {
        networkService = new NetworkService(Collections.emptyList());
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        threadPool = new TestThreadPool("test");
    }

    @After
    public void shutdown() {
        terminate(threadPool);
    }

    public void testOversizedUploadGetsResponseThenCleanEndOfStream() throws Exception {
        final Encoding encoding = randomFrom(Encoding.values());
        final boolean chunked = randomBoolean();
        // a keep-alive request with a too large Content-Length that is not compressed is rejected without closing
        final boolean connectionClose = chunked == false && encoding == Encoding.IDENTITY || randomBoolean();
        final RequestBuilder request = post().body(encoding, incompressible(4 * LIMIT)).chunked(chunked);
        if (connectionClose) {
            request.header("Connection", "close");
        }
        try (TestTransport transport = startTransport(settings()); Netty4HttpRawClient client = connect(transport)) {
            final CompletableFuture<Void> upload = sendInBackground(client, request.bytes(), between(1, 16 * 1024));
            assertTerminalRejection(client);
            upload.get(Netty4HttpRawClient.IO_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS); // all of it was accepted, no reset
            assertThat(dispatched.get(), equalTo(0));
        }
    }

    public void testDecodedSizeIsLimitedWhenEncodedSizeIsWithinLimit() throws Exception {
        final Encoding encoding = randomFrom(Encoding.GZIP, Encoding.DEFLATE);
        final RequestBuilder request = post().body(encoding, compressible(8 * LIMIT)).chunked(randomBoolean());
        assertThat(request.encodedBodyLength(), lessThan(LIMIT));
        try (TestTransport transport = startTransport(settings()); Netty4HttpRawClient client = connect(transport)) {
            final CompletableFuture<Void> upload = sendInBackground(client, request.bytes(), between(1, 16 * 1024));
            assertTerminalRejection(client);
            upload.get(Netty4HttpRawClient.IO_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    public void testEncodedContentLengthIsLimitedWhenDecodedSizeIsWithinLimit() throws Exception {
        // stored deflate blocks: the body as sent is a little larger than the decoded body, which is exactly at the limit
        final byte[] decoded = incompressible(LIMIT);
        final ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        try (DeflaterOutputStream out = new DeflaterOutputStream(encoded, new Deflater(Deflater.NO_COMPRESSION))) {
            out.write(decoded);
        }
        final RequestBuilder request = post().header("Content-Encoding", "deflate").rawBody(encoded.toByteArray());
        assertThat(request.encodedBodyLength(), greaterThan(LIMIT));
        try (TestTransport transport = startTransport(settings()); Netty4HttpRawClient client = connect(transport)) {
            client.write(request.head());
            // rejected on the headers alone, before any of the body is sent
            final Response response = client.readResponse();
            assertThat(response.status(), equalTo(413));
            assertTrue(response.connectionClose());
            client.write(request.wireBody());
            client.readEndOfStream();
            assertThat(dispatched.get(), equalTo(0));
        }
    }

    public void testExpectContinueWithTooLargeContentLengthIsRejectedAndConnectionReused() throws Exception {
        final Encoding encoding = randomFrom(Encoding.values());
        final RequestBuilder request = post().header("Expect", "100-continue").body(encoding, incompressible(2 * LIMIT));
        assertThat(request.encodedBodyLength(), greaterThan(LIMIT));
        try (TestTransport transport = startTransport(settings()); Netty4HttpRawClient client = connect(transport)) {
            client.write(request.head());
            final Response response = client.readResponse();
            assertThat(response.status(), equalTo(413)); // and no 100 Continue before it
            assertFalse(response.connectionClose());
            assertConnectionUsable(client);
            assertRejectionHandlerRemoved(transport.hold);
        }
    }

    public void testKeepAliveRejectionReusesConnection() throws Exception {
        final RequestBuilder request = post().body(Encoding.IDENTITY, incompressible(LIMIT + 1));
        try (TestTransport transport = startTransport(settings()); Netty4HttpRawClient client = connect(transport)) {
            client.write(request.bytes());
            final Response response = client.readResponse();
            assertThat(response.status(), equalTo(413));
            assertFalse(response.connectionClose());
            assertConnectionUsable(client);
            assertRejectionHandlerRemoved(transport.hold);
        }
    }

    public void testRequestsAtTheLimitSucceed() throws Exception {
        try (TestTransport transport = startTransport(settings())) {
            for (Encoding encoding : Encoding.values()) {
                for (boolean chunked : new boolean[] { false, true }) {
                    final byte[] body = randomBoolean() ? compressible(LIMIT) : incompressible(LIMIT);
                    try (Netty4HttpRawClient client = connect(transport)) {
                        client.writeInPieces(post().body(encoding, body).chunked(chunked).bytes(), between(1, 16 * 1024));
                        final Response response = client.readResponse();
                        assertThat(encoding + "/chunked=" + chunked, response.status(), equalTo(200));
                        assertThat(new String(response.body(), StandardCharsets.UTF_8), equalTo(Integer.toString(LIMIT)));
                    }
                }
            }
        }
    }

    public void testRequestsPipelinedAfterTerminalRejectionAreNotProcessed() throws Exception {
        final ByteArrayOutputStream wire = new ByteArrayOutputStream();
        wire.writeBytes(post().body(Encoding.IDENTITY, incompressible(2 * LIMIT)).chunked(true).bytes());
        wire.writeBytes(post().body(Encoding.IDENTITY, "small".getBytes(StandardCharsets.UTF_8)).bytes());
        try (TestTransport transport = startTransport(settings()); Netty4HttpRawClient client = connect(transport)) {
            client.write(wire.toByteArray());
            assertTerminalRejection(client);
            assertThat(dispatched.get(), equalTo(0));
        }
    }

    public void testClientFinWhileResponseIsPending() throws Exception {
        try (TestTransport transport = startTransport(settings()); Netty4HttpRawClient client = connect(transport)) {
            transport.hold.enable();
            client.write(post().body(Encoding.IDENTITY, incompressible(2 * LIMIT)).chunked(true).bytes());
            client.shutdownOutput();
            transport.hold.awaitHeld();
            assertBusy(() -> assertTrue(((DuplexChannel) transport.hold.channel).isInputShutdown()));
            transport.hold.release();
            assertTerminalRejection(client);
        }
    }

    public void testReadTimeoutWhileResponseIsPending() throws Exception {
        final Settings settings = Settings.builder()
            .put(settings())
            .put(HttpTransportSettings.SETTING_HTTP_READ_TIMEOUT.getKey(), "200ms")
            .build();
        try (TestTransport transport = startTransport(settings); Netty4HttpRawClient client = connect(transport)) {
            transport.hold.enable();
            final byte[] wire = post().body(Encoding.IDENTITY, incompressible(2 * LIMIT)).chunked(true).bytes();
            client.write(wire, 0, wire.length - 1024); // the client stalls in the middle of its body
            transport.hold.awaitHeld();
            assertTrue(transport.hold.closeRequested.await(10, TimeUnit.SECONDS)); // by the read timeout
            transport.hold.release();
            assertTerminalRejection(client);
        }
    }

    public void testDecoderFailureWhileResponseIsPending() throws Exception {
        // the first chunk alone exceeds the limit once decoded, the second is not valid gzip
        final ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        final GZIPOutputStream gzip = new GZIPOutputStream(encoded, true);
        gzip.write(compressible(2 * LIMIT));
        gzip.flush();
        final byte[] valid = encoded.toByteArray();
        final byte[] invalid = new byte[64];
        Arrays.fill(invalid, (byte) 0xff);
        final ByteArrayOutputStream wire = new ByteArrayOutputStream();
        wire.writeBytes(post().header("Content-Encoding", "gzip").chunked(true).head());
        for (byte[] chunk : new byte[][] { valid, invalid }) {
            wire.writeBytes((Integer.toHexString(chunk.length) + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
            wire.writeBytes(chunk);
            wire.writeBytes("\r\n".getBytes(StandardCharsets.ISO_8859_1));
        }
        try (TestTransport transport = startTransport(settings()); Netty4HttpRawClient client = connect(transport)) {
            transport.hold.enable();
            client.write(wire.toByteArray()); // one write, so the failure is decoded right after the rejection
            transport.hold.awaitHeld();
            assertTrue(transport.decoderFailure.await(10, TimeUnit.SECONDS));
            transport.hold.release();
            assertTerminalRejection(client);
        }
    }

    public void testDrainEndsWhenClientCloses() throws Exception {
        try (TestTransport transport = startTransport(settings()); Netty4HttpRawClient client = connect(transport)) {
            final byte[] wire = post().body(Encoding.IDENTITY, incompressible(2 * LIMIT)).chunked(true).bytes();
            client.write(wire, 0, wire.length * 3 / 4); // well past the limit, but not the whole body
            assertTerminalRejection(client);
            assertTrue(transport.hold.channel.isOpen()); // draining, with the default 5s idle timeout
            client.shutdownOutput();
            assertBusy(() -> assertFalse(transport.hold.channel.isOpen()), 2, TimeUnit.SECONDS);
        }
    }

    public void testDrainEndsWhenClientIsIdle() throws Exception {
        try (
            TestTransport transport = startTransport(settings(), timeouts(10_000, 300, 30_000));
            Netty4HttpRawClient client = connect(transport)
        ) {
            final byte[] wire = post().body(Encoding.IDENTITY, incompressible(2 * LIMIT)).chunked(true).bytes();
            client.write(wire, 0, wire.length * 3 / 4); // well past the limit, but not the whole body
            assertTerminalRejection(client);
            assertBusy(() -> assertFalse(transport.hold.channel.isOpen()), 5, TimeUnit.SECONDS);
        }
    }

    public void testDrainIsBoundedForAContinuousSender() throws Exception {
        final long drainMaxMillis = 1_000;
        try (
            TestTransport transport = startTransport(settings(), timeouts(10_000, 500, drainMaxMillis));
            Netty4HttpRawClient client = connect(transport)
        ) {
            client.write(post().chunked(true).head());
            final byte[] chunk = ("100\r\n" + "x".repeat(256) + "\r\n").getBytes(StandardCharsets.ISO_8859_1);
            for (int i = 0; i <= LIMIT / 256; i++) {
                client.write(chunk);
            }
            final CompletableFuture<Void> trickle = CompletableFuture.runAsync(() -> {
                try {
                    while (true) {
                        client.write(chunk);
                        Thread.sleep(50); // never idle for the 500ms idle timeout
                    }
                } catch (IOException e) {
                    // the server eventually closes while the client is still sending
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertTerminalRejection(client);
            final long drainStart = System.nanoTime();
            assertBusy(() -> assertFalse(transport.hold.channel.isOpen()), 10, TimeUnit.SECONDS);
            final long drainedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - drainStart);
            assertThat(drainedMillis, greaterThanOrEqualTo(drainMaxMillis / 2));
            client.close();
            trickle.get(10, TimeUnit.SECONDS);
        }
    }

    public void testStoppingTheTransportClosesDrainingConnections() throws Exception {
        try (TestTransport transport = startTransport(settings()); Netty4HttpRawClient client = connect(transport)) {
            final byte[] wire = post().body(Encoding.IDENTITY, incompressible(2 * LIMIT)).chunked(true).bytes();
            client.write(wire, 0, wire.length * 3 / 4); // well past the limit, but not the whole body
            assertTerminalRejection(client);
            assertTrue(transport.hold.channel.isOpen());
            transport.stop();
            assertBusy(() -> assertFalse(transport.hold.channel.isOpen()), 5, TimeUnit.SECONDS);
        }
    }

    private static void assertTerminalRejection(Netty4HttpRawClient client) throws IOException {
        final Response response = client.readResponse();
        assertThat(response.status(), equalTo(413));
        assertTrue("expected Connection: close in " + response.headers(), response.connectionClose());
        client.readEndOfStream(); // a FIN, not a reset
    }

    private void assertConnectionUsable(Netty4HttpRawClient client) throws IOException {
        client.write(post().body(Encoding.IDENTITY, "small".getBytes(StandardCharsets.UTF_8)).bytes());
        final Response response = client.readResponse();
        assertThat(response.status(), equalTo(200));
        assertThat(new String(response.body(), StandardCharsets.UTF_8), equalTo("5"));
    }

    private static void assertRejectionHandlerRemoved(ResponseHold hold) throws Exception {
        assertBusy(() -> {
            assertThat(hold.channel.pipeline().get(Netty4HttpRejectionHandler.class), nullValue());
            assertFalse(((SocketChannelConfig) hold.channel.config()).isAllowHalfClosure());
        });
    }

    private static RequestBuilder post() {
        return new RequestBuilder("POST", "/");
    }

    private static byte[] incompressible(int length) {
        return randomAlphaOfLength(length).getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] compressible(int length) {
        return "a".repeat(length).getBytes(StandardCharsets.US_ASCII);
    }

    private static CompletableFuture<Void> sendInBackground(Netty4HttpRawClient client, byte[] bytes, int writeSize) {
        return CompletableFuture.runAsync(() -> {
            try {
                client.writeInPieces(bytes, writeSize);
            } catch (IOException e) {
                throw new AssertionError("the upload failed", e);
            }
        });
    }

    private static Settings settings() {
        return Settings.builder()
            .put(HttpTransportSettings.SETTING_HTTP_PORT.getKey(), getPortRange())
            .put(HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH.getKey(), LIMIT + "b")
            .build();
    }

    private static Netty4HttpRejectionHandler.Timeouts timeouts(long writeMillis, long drainIdleMillis, long drainMaxMillis) {
        return new Netty4HttpRejectionHandler.Timeouts(
            TimeUnit.MILLISECONDS.toNanos(writeMillis),
            TimeUnit.MILLISECONDS.toNanos(drainIdleMillis),
            TimeUnit.MILLISECONDS.toNanos(drainMaxMillis)
        );
    }

    private TestTransport startTransport(Settings settings) {
        return startTransport(settings, Netty4HttpRejectionHandler.Timeouts.DEFAULT);
    }

    private TestTransport startTransport(Settings settings, Netty4HttpRejectionHandler.Timeouts timeouts) {
        final HttpServerTransport.Dispatcher dispatcher = dispatcherBuilderWithDefaults().withDispatchRequest((request, channel, ctx) -> {
            dispatched.incrementAndGet();
            final String length = Integer.toString(request.content().length());
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, new BytesArray(length)));
        }).build();
        final TestTransport transport = new TestTransport(settings, dispatcher, timeouts);
        transport.start();
        return transport;
    }

    private static Netty4HttpRawClient connect(TestTransport transport) throws IOException {
        return Netty4HttpRawClient.connect(transport.boundAddress().boundAddresses());
    }

    private class TestTransport extends Netty4HttpServerTransport {
        final ResponseHold hold = new ResponseHold();
        final CountDownLatch decoderFailure = new CountDownLatch(1);

        TestTransport(Settings settings, Dispatcher dispatcher, Netty4HttpRejectionHandler.Timeouts timeouts) {
            super(
                settings,
                Netty4HttpOversizedRequestTests.this.networkService,
                Netty4HttpOversizedRequestTests.this.bigArrays,
                Netty4HttpOversizedRequestTests.this.threadPool,
                xContentRegistry(),
                dispatcher,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                new SharedGroupFactory(settings),
                NoopTracer.INSTANCE,
                timeouts
            );
        }

        @Override
        public ChannelHandler configureServerChannelHandler() {
            return new HttpChannelHandler(this, handlingSettings) {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    super.initChannel(ch);
                    ch.pipeline().addFirst("test_hold", hold);
                }
            };
        }

        @Override
        protected ChannelInboundHandlerAdapter createDecompressor() {
            return new HttpContentDecompressor(0) {
                @Override
                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                    decoderFailure.countDown();
                    super.exceptionCaught(ctx, cause);
                }
            };
        }
    }

    /**
     * Sits at the network edge of the pipeline (once the rejection handler is installed, right behind it). When enabled it
     * holds the encoded 413 and everything written after it until released, and it records close requests on their way
     * to the socket. Each test uses a single connection.
     */
    @ChannelHandler.Sharable
    private static final class ResponseHold extends ChannelDuplexHandler {
        private final CountDownLatch held = new CountDownLatch(1);
        final CountDownLatch closeRequested = new CountDownLatch(1);
        private final Queue<Map.Entry<Object, ChannelPromise>> heldWrites = new ArrayDeque<>();
        private volatile boolean enabled;
        private boolean holding;
        volatile Channel channel;
        private ChannelHandlerContext context;

        void enable() {
            enabled = true;
        }

        void awaitHeld() throws InterruptedException {
            assertTrue("the 413 was not written", held.await(10, TimeUnit.SECONDS));
        }

        void release() {
            channel.eventLoop().execute(() -> {
                holding = false;
                Map.Entry<Object, ChannelPromise> write;
                while ((write = heldWrites.poll()) != null) {
                    context.write(write.getKey(), write.getValue());
                }
                context.flush();
            });
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            context = ctx;
            channel = ctx.channel();
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            if (holding == false
                && enabled
                && msg instanceof ByteBuf buf
                && buf.toString(StandardCharsets.ISO_8859_1).startsWith("HTTP/1.1 413")) {
                holding = true;
                held.countDown();
            }
            if (holding) {
                heldWrites.add(Map.entry(msg, promise));
            } else {
                ctx.write(msg, promise);
            }
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            closeRequested.countDown();
            super.close(ctx, promise);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            Map.Entry<Object, ChannelPromise> write;
            while ((write = heldWrites.poll()) != null) {
                ReferenceCountUtil.release(write.getKey());
                write.getValue().tryFailure(new IOException("connection closed while the write was held"));
            }
            super.channelInactive(ctx);
        }
    }
}

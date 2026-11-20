/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.common.util.io.IOUtils;
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
import java.util.Arrays;
import java.util.Collections;
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
import io.netty.channel.PendingWriteQueue;
import io.netty.channel.socket.DuplexChannel;
import io.netty.handler.codec.http.HttpContentDecompressor;

import static org.opensearch.http.TestDispatcherBuilder.dispatcherBuilderWithDefaults;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

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
    private TestTransport transport;
    private Netty4HttpRawClient client;
    private final AtomicInteger dispatched = new AtomicInteger();

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(Collections.emptyList());
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        threadPool = new TestThreadPool("test");
        start(settings());
    }

    @After
    public void shutdown() throws IOException {
        try {
            IOUtils.close(client, transport);
        } finally {
            terminate(threadPool);
        }
    }

    public void testOversizedUploadGetsResponseThenCleanEndOfStream() throws Exception {
        for (Encoding encoding : Encoding.values()) {
            for (boolean chunked : new boolean[] { false, true }) {
                reconnect();
                final RequestBuilder request = post().body(encoding, incompressible(4 * LIMIT)).chunked(chunked);
                // An identity request with an oversized Content-Length is otherwise recoverable.
                if (chunked == false && encoding == Encoding.IDENTITY) {
                    request.header("Connection", "close");
                }
                assertUploadRejected(request.bytes());
            }
        }
    }

    public void testPartialOversizedUploadDrainsAfterResponse() throws Exception {
        for (boolean upgrade : new boolean[] { false, true }) {
            for (boolean chunked : new boolean[] { false, true }) {
                reconnect();
                final RequestBuilder request = (upgrade ? h2cUpgrade(post()) : post()).body(Encoding.IDENTITY, incompressible(2 * LIMIT))
                    .chunked(chunked);
                if (chunked == false) {
                    request.header("Connection", "close");
                }
                final byte[] wire = request.bytes();
                client.write(wire, 0, wire.length * 3 / 4);
                assertTerminalRejection();
                assertTrue("The unfinished upload must still be draining", transport.hold.channel.isOpen());
                assertNotNull(transport.hold.channel.pipeline().get(Netty4HttpRejectionHandler.class));
            }
        }
    }

    public void testOversizedUpgradeUploadGetsResponseThenCleanEndOfStream() throws Exception {
        assertUploadRejected(h2cUpgrade(post().body(Encoding.IDENTITY, incompressible(8 * LIMIT)).chunked(true)).bytes());
    }

    public void testDecodedSizeIsLimitedWhenEncodedSizeIsWithinLimit() throws Exception {
        for (Encoding encoding : new Encoding[] { Encoding.GZIP, Encoding.DEFLATE }) {
            for (boolean chunked : new boolean[] { false, true }) {
                for (boolean expectContinue : new boolean[] { false, true }) {
                    reconnect();
                    final RequestBuilder request = post().body(encoding, compressible(8 * LIMIT)).chunked(chunked);
                    assertThat(request.encodedBodyLength(), lessThan(LIMIT));
                    if (expectContinue) {
                        client.write(request.header("Expect", "100-continue").head());
                        assertThat(client.readResponse().status(), equalTo(100));
                        assertUploadRejected(request.wireBody());
                    } else {
                        assertUploadRejected(request.bytes());
                    }
                }
            }
        }
    }

    /** Uploads in the background while reading the rejection: all of the upload must be accepted, without a reset. */
    private void assertUploadRejected(byte[] bytes) throws Exception {
        final int writeSize = between(1, 16 * 1024);
        final CompletableFuture<Void> upload = CompletableFuture.runAsync(() -> {
            try {
                client.writeInPieces(bytes, writeSize);
            } catch (IOException e) {
                throw new AssertionError("the upload failed", e);
            }
        });
        assertTerminalRejection();
        upload.get(Netty4HttpRawClient.IO_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    public void testEncodedContentLengthIsLimitedWhenDecodedSizeIsWithinLimit() throws Exception {
        // stored deflate blocks: the body as sent is a little larger than the decoded body, which is exactly at the limit
        final byte[] decoded = incompressible(LIMIT);
        final ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        final Deflater deflater = new Deflater(Deflater.NO_COMPRESSION);
        try (DeflaterOutputStream out = new DeflaterOutputStream(encoded, deflater)) {
            out.write(decoded);
        } finally {
            deflater.end();
        }
        final RequestBuilder request = post().header("Content-Encoding", "deflate").rawBody(encoded.toByteArray());
        assertThat(request.encodedBodyLength(), greaterThan(LIMIT));
        client.write(request.head());
        // Reject from the headers, then drain the upload without decompressing it.
        final Response rejection = client.readResponse();
        assertThat(rejection.status(), equalTo(413));
        assertTrue(rejection.connectionClose());
        client.write(request.wireBody());
        client.readEndOfStream();
        assertThat(dispatched.get(), equalTo(0));

        assertRecoverableRejection(request.header("Expect", "100-continue").head());
        assertRecoverableRejection(request.header("Connection", "close").head());

        // Unknown encoded lengths retain the decoded-size limit.
        reconnect();
        client.writeInPieces(
            post().header("Content-Encoding", "deflate").rawBody(encoded.toByteArray()).chunked(true).bytes(),
            between(1, 16 * 1024)
        );
        final Response response = client.readResponse();
        assertThat(response.status(), equalTo(200));
        assertThat(new String(response.body(), StandardCharsets.UTF_8), equalTo(Integer.toString(LIMIT)));
    }

    public void testRecoverableRejectionsReuseConnection() throws Exception {
        for (boolean upgrade : new boolean[] { false, true }) {
            final RequestBuilder request = upgrade ? h2cUpgrade(post()) : post();
            request.header("Expect", "100-continue").body(Encoding.IDENTITY, incompressible(2 * LIMIT));
            assertRecoverableRejection(request.head());
            // Preserve Netty's expectation handling even when the client asks to close.
            assertRecoverableRejection(request.header("Connection", "close").head());
            assertRecoverableRejection((upgrade ? h2cUpgrade(post()) : post()).body(Encoding.IDENTITY, incompressible(LIMIT + 1)).bytes());
        }
    }

    public void testCompressedExpectRejectionsReuseConnection() throws Exception {
        for (Encoding encoding : new Encoding[] { Encoding.GZIP, Encoding.DEFLATE }) {
            final RequestBuilder request = post().header("Expect", "100-continue").body(encoding, incompressible(2 * LIMIT));
            assertThat(request.encodedBodyLength(), greaterThan(LIMIT));
            assertRecoverableRejection(request.head());
            assertRecoverableRejection(request.header("Connection", "close").head());
        }
    }

    private void assertRecoverableRejection(byte[] request) throws Exception {
        reconnect();
        client.write(request);
        final Response rejection = client.readResponse();
        assertThat(rejection.status(), equalTo(413)); // no preceding 100 Continue
        assertFalse(rejection.connectionClose());
        // the connection remains usable
        // Missing Content-Length on the next request must replace the rejected request's recorded length.
        client.write(post().body(Encoding.IDENTITY, "small".getBytes(StandardCharsets.UTF_8)).chunked(true).bytes());
        final Response response = client.readResponse();
        assertThat(response.status(), equalTo(200));
        assertThat(new String(response.body(), StandardCharsets.UTF_8), equalTo("5"));
        assertNull(transport.hold.channel.pipeline().get(Netty4HttpRejectionHandler.class));
    }

    public void testRequestsAtTheLimitSucceed() throws Exception {
        for (Encoding encoding : Encoding.values()) {
            for (boolean chunked : new boolean[] { false, true }) {
                final byte[] body = encoding == Encoding.IDENTITY ? incompressible(LIMIT) : compressible(LIMIT);
                reconnect();
                client.writeInPieces(post().body(encoding, body).chunked(chunked).bytes(), between(1, 16 * 1024));
                final Response response = client.readResponse();
                assertThat(encoding + "/chunked=" + chunked, response.status(), equalTo(200));
                assertThat(new String(response.body(), StandardCharsets.UTF_8), equalTo(Integer.toString(LIMIT)));
            }
        }
    }

    public void testRequestsPipelinedAfterTerminalRejectionAreNotProcessed() throws Exception {
        // Small enough to arrive in one read, so the server decodes the pipelined request along with the rejected one.
        start(Settings.builder().put(settings()).put(HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH.getKey(), "1kb").build());
        for (boolean upgrade : new boolean[] { false, true }) {
            reconnect();
            final ByteArrayOutputStream wire = new ByteArrayOutputStream();
            wire.writeBytes((upgrade ? h2cUpgrade(post()) : post()).body(Encoding.IDENTITY, incompressible(2048)).chunked(true).bytes());
            wire.writeBytes(post().body(Encoding.IDENTITY, "small".getBytes(StandardCharsets.UTF_8)).bytes());
            client.write(wire.toByteArray());
            assertTerminalRejection();
            // The response is on the wire before the server handles the rest of that read: wait for it to finish.
            transport.hold.channel.eventLoop().submit(() -> {}).get(10, TimeUnit.SECONDS);
            assertThat(dispatched.get(), equalTo(0));
        }
    }

    public void testClientFinWhileResponseIsPending() throws Exception {
        assertDeliveredDespite(oversizedChunkedUpload(), () -> {
            client.shutdownOutput();
            assertBusy(() -> assertTrue(((DuplexChannel) transport.hold.channel).isInputShutdown()));
        });
    }

    public void testReadTimeoutWhileResponseIsPending() throws Exception {
        start(Settings.builder().put(settings()).put(HttpTransportSettings.SETTING_HTTP_READ_TIMEOUT.getKey(), "200ms").build());
        final byte[] wire = oversizedChunkedUpload();
        // Stall in the middle of the upload and wait for the real read timeout to request closure.
        assertDeliveredDespite(
            Arrays.copyOf(wire, wire.length - 1024),
            () -> assertTrue(transport.hold.closeRequested.await(10, TimeUnit.SECONDS))
        );
    }

    public void testDecoderFailureWhileResponseIsPending() throws Exception {
        // One compressed chunk exceeds the limit before the decoder encounters its invalid remainder.
        final ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        final byte[] valid;
        try (GZIPOutputStream gzip = new GZIPOutputStream(encoded, true)) {
            gzip.write(compressible(2 * LIMIT));
            gzip.flush();
            valid = encoded.toByteArray();
        }
        final byte[] invalid = Arrays.copyOf(valid, valid.length + 64);
        Arrays.fill(invalid, valid.length, invalid.length, (byte) 0xff);
        final ByteArrayOutputStream wire = new ByteArrayOutputStream();
        wire.writeBytes(post().header("Content-Encoding", "gzip").chunked(true).head());
        RequestBuilder.writeChunk(wire, invalid, 0, invalid.length);
        assertDeliveredDespite(wire.toByteArray(), () -> {
            assertTrue(transport.decoderFailure.await(10, TimeUnit.SECONDS));
            transport.hold.channel.eventLoop().submit(() -> {}).get(10, TimeUnit.SECONDS);
            assertEquals("The rejected body's decoder failure must not reach the transport", 1, transport.hold.closeRequested.getCount());
        });
    }

    /** Holds the 413 on the server until {@code trigger} has made the competing event happen there. */
    private void assertDeliveredDespite(byte[] upload, CheckedRunnable<Exception> trigger) throws Exception {
        transport.hold.enable();
        client.write(upload);
        transport.hold.awaitHeld();
        trigger.run();
        transport.hold.release();
        assertTerminalRejection();
    }

    public void testDrainEndsWhenClientCloses() throws Exception {
        startDraining();
        client.shutdownOutput();
        assertBusy(() -> assertFalse(transport.hold.channel.isOpen()), 2, TimeUnit.SECONDS);
    }

    public void testStoppingTheTransportClosesDrainingConnections() throws Exception {
        startDraining();
        final CompletableFuture<Void> stopping = CompletableFuture.runAsync(transport::stop);
        try {
            assertTrue(
                "Transport stop must not wait for the drain deadline",
                transport.hold.channel.closeFuture().await(2, TimeUnit.SECONDS)
            );
        } finally {
            stopping.get(10, TimeUnit.SECONDS);
        }
    }

    private void startDraining() throws IOException {
        final byte[] wire = oversizedChunkedUpload();
        client.write(wire, 0, wire.length * 3 / 4); // exceed the limit without finishing the upload
        assertTerminalRejection();
        assertTrue(transport.hold.channel.isOpen());
    }

    private void assertTerminalRejection() throws IOException {
        final Response response = client.readResponse();
        assertThat(response.status(), equalTo(413));
        assertTrue("expected Connection: close in " + response.headers(), response.connectionClose());
        client.readEndOfStream(); // a FIN, not a reset
        assertThat(dispatched.get(), equalTo(0));
    }

    private static RequestBuilder post() {
        return new RequestBuilder("POST", "/");
    }

    private static RequestBuilder h2cUpgrade(RequestBuilder request) {
        return request.header("Connection", "Upgrade, HTTP2-Settings")
            .header("Upgrade", "h2c")
            .header("HTTP2-Settings", "AAMAAABkAARAAAAAAAIAAAAA");
    }

    /** Twice the limit, chunked: rejected while the body is arriving, so the connection must close. */
    private static byte[] oversizedChunkedUpload() {
        return post().body(Encoding.IDENTITY, incompressible(2 * LIMIT)).chunked(true).bytes();
    }

    private static byte[] incompressible(int length) {
        return randomAlphaOfLength(length).getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] compressible(int length) {
        return "a".repeat(length).getBytes(StandardCharsets.US_ASCII);
    }

    private static Settings settings() {
        return Settings.builder()
            .put(HttpTransportSettings.SETTING_HTTP_PORT.getKey(), getPortRange())
            .put(HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH.getKey(), LIMIT + "b")
            .build();
    }

    /** Starts a transport and connects the client to it, replacing the ones {@link #setup()} started. */
    private void start(Settings settings) throws Exception {
        IOUtils.close(client, transport);
        final HttpServerTransport.Dispatcher dispatcher = dispatcherBuilderWithDefaults().withDispatchRequest((request, channel, ctx) -> {
            dispatched.incrementAndGet();
            final String length = Integer.toString(request.content().length());
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, new BytesArray(length)));
        }).build();
        transport = new TestTransport(settings, dispatcher);
        transport.start();
        reconnect();
    }

    /** Replaces the client with one on a new connection. */
    private void reconnect() throws Exception {
        IOUtils.close(client);
        final Channel previous = transport.hold.channel;
        client = Netty4HttpRawClient.connect(transport.boundAddress().boundAddresses());
        // Finish server initialization before another reconnect can register a channel on a different event loop.
        assertBusy(() -> assertNotSame(previous, transport.hold.channel));
    }

    private class TestTransport extends Netty4HttpServerTransport {
        final ResponseHold hold = new ResponseHold();
        final CountDownLatch decoderFailure = new CountDownLatch(1);

        TestTransport(Settings settings, Dispatcher dispatcher) {
            super(
                settings,
                Netty4HttpOversizedRequestTests.this.networkService,
                Netty4HttpOversizedRequestTests.this.bigArrays,
                Netty4HttpOversizedRequestTests.this.threadPool,
                xContentRegistry(),
                dispatcher,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                new SharedGroupFactory(settings),
                NoopTracer.INSTANCE
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
     * to the socket. It tracks the most recent connection.
     */
    @ChannelHandler.Sharable
    private static final class ResponseHold extends ChannelDuplexHandler {
        private final CountDownLatch held = new CountDownLatch(1);
        final CountDownLatch closeRequested = new CountDownLatch(1);
        private PendingWriteQueue heldWrites;
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
            context.executor().execute(() -> {
                holding = false;
                heldWrites.removeAndWriteAll();
                context.flush();
            });
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            channel = ctx.channel();
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            if (holding == false
                && enabled
                && msg instanceof ByteBuf buf
                && buf.toString(StandardCharsets.ISO_8859_1).startsWith("HTTP/1.1 413")) {
                context = ctx;
                heldWrites = new PendingWriteQueue(ctx);
                holding = true;
                held.countDown();
            }
            if (holding && ctx == context) {
                heldWrites.add(msg, promise);
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
            if (ctx == context) {
                heldWrites.removeAndFailAll(new IOException("connection closed while the write was held"));
            }
            super.channelInactive(ctx);
        }
    }
}

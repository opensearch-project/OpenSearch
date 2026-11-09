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

import org.opensearch.common.Randomness;
import org.opensearch.common.io.stream.ReleasableBytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.http.HttpPipelinedRequest;
import org.opensearch.http.HttpPipelinedResponse;
import org.opensearch.http.HttpResponse;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.ReferenceCounted;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class Netty4HttpPipeliningHandlerTests extends OpenSearchTestCase {

    private final ExecutorService handlerService = Executors.newFixedThreadPool(randomIntBetween(4, 8));
    private final ExecutorService eventLoopService = Executors.newFixedThreadPool(1);
    private final Map<String, CountDownLatch> waitingRequests = new ConcurrentHashMap<>();
    private final Map<String, CountDownLatch> finishingRequests = new ConcurrentHashMap<>();

    @After
    public void tearDown() throws Exception {
        waitingRequests.keySet().forEach(this::finishRequest);
        shutdownExecutorServices();
        super.tearDown();
    }

    private CountDownLatch finishRequest(String url) {
        waitingRequests.get(url).countDown();
        return finishingRequests.get(url);
    }

    private void shutdownExecutorServices() throws InterruptedException {
        if (!handlerService.isShutdown()) {
            handlerService.shutdown();
            handlerService.awaitTermination(10, TimeUnit.SECONDS);
        }
        if (!eventLoopService.isShutdown()) {
            eventLoopService.shutdown();
            eventLoopService.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    public void testPipelinedRequestsCreateReferenceCountedResponses() {
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(new Netty4HttpPipeliningHandler(logger, 2));
        try {
            for (int sequence = 0; sequence < 2; sequence++) {
                final Netty4HttpRequest request = createHttpRequest("/" + sequence);
                try {
                    assertTrue(embeddedChannel.writeInbound(request));
                    final HttpPipelinedRequest pipelinedRequest = embeddedChannel.readInbound();
                    assertNotNull(pipelinedRequest);
                    assertEquals(sequence, pipelinedRequest.getSequence());

                    final String content = "response-" + sequence;
                    final HttpPipelinedResponse response = pipelinedRequest.createResponse(RestStatus.OK, new BytesArray(content));
                    final FullHttpResponse delegate = (FullHttpResponse) response.getDelegateRequest();
                    try {
                        assertThat(response, instanceOf(ReferenceCounted.class));
                        assertEquals(sequence, response.getSequence());
                        assertEquals(RestStatus.OK.getStatus(), delegate.status().code());
                        assertEquals(content, delegate.content().toString(StandardCharsets.UTF_8));
                        response.addHeader("x-response-sequence", Integer.toString(sequence));
                        assertTrue(response.containsHeader("x-response-sequence"));
                        assertEquals(Integer.toString(sequence), delegate.headers().get("x-response-sequence"));
                        assertEquals("Pipelining must not retain the response", 1, delegate.refCnt());
                        final ReferenceCounted referenceCounted = (ReferenceCounted) response;
                        assertEquals(1, referenceCounted.refCnt());
                        assertSame(response, referenceCounted.retain());
                        assertEquals(2, delegate.refCnt());
                        assertSame(response, referenceCounted.retain(2));
                        assertEquals(4, delegate.refCnt());
                        assertSame(response, referenceCounted.touch());
                        assertSame(response, referenceCounted.touch("pipelined response"));
                        assertFalse(referenceCounted.release(3));
                        assertEquals(1, delegate.refCnt());
                        assertTrue(referenceCounted.release());
                        assertEquals(0, delegate.refCnt());
                    } finally {
                        if (delegate.refCnt() > 0) {
                            delegate.release();
                        }
                    }
                } finally {
                    request.release();
                }
            }
        } finally {
            embeddedChannel.finishAndReleaseAll();
        }
    }

    public void testPipelinedResponseIsReleasedAfterPipelineTeardown() {
        assertRejectedPipelinedResponseIsReleased(true);
    }

    public void testCancelledPipelinedResponseIsReleasedBeforeHandler() {
        assertRejectedPipelinedResponseIsReleased(false);
    }

    private void assertRejectedPipelinedResponseIsReleased(boolean closeBeforeWrite) {
        final Netty4HttpPipeliningHandler handler = new Netty4HttpPipeliningHandler(logger, 2);
        final EmbeddedChannel channel = new EmbeddedChannel(handler);
        final Netty4HttpRequest request = createHttpRequest("/");
        FullHttpResponse delegate = null;
        try {
            channel.writeInbound(request);
            final HttpPipelinedRequest pipelinedRequest = channel.readInbound();
            final HttpPipelinedResponse response = pipelinedRequest.createResponse(RestStatus.OK, new BytesArray("response"));
            delegate = (FullHttpResponse) response.getDelegateRequest();
            final ChannelPromise promise = channel.newPromise();
            if (closeBeforeWrite) {
                channel.close();
                channel.runPendingTasks();
                assertNull(channel.pipeline().context(handler));
            } else {
                promise.cancel(false);
            }

            channel.writeAndFlush(response, promise);
            assertTrue(promise.isDone());
            if (closeBeforeWrite) {
                assertThat(promise.cause(), instanceOf(ClosedChannelException.class));
            } else {
                assertThat(promise.cause(), instanceOf(CancellationException.class));
            }
            assertEquals("Netty must be able to release the wrapper without invoking the pipelining handler", 0, delegate.refCnt());
            assertNull(channel.readOutbound());
        } finally {
            request.release();
            channel.finishAndReleaseAll();
            if (delegate != null && delegate.refCnt() > 0) {
                delegate.release();
            }
        }
    }

    public void testThatPipeliningWorksWithFastSerializedRequests() throws InterruptedException {
        final int numberOfRequests = randomIntBetween(2, 128);
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(
            new Netty4HttpPipeliningHandler(logger, numberOfRequests),
            new WorkEmulatorHandler()
        );

        for (int i = 0; i < numberOfRequests; i++) {
            embeddedChannel.writeInbound(createHttpRequest("/" + String.valueOf(i)));
        }

        final List<CountDownLatch> latches = new ArrayList<>();
        for (final String url : waitingRequests.keySet()) {
            latches.add(finishRequest(url));
        }

        for (final CountDownLatch latch : latches) {
            latch.await();
        }

        embeddedChannel.flush();

        for (int i = 0; i < numberOfRequests; i++) {
            assertReadHttpMessageHasContent(embeddedChannel, String.valueOf(i));
        }

        assertTrue(embeddedChannel.isOpen());
    }

    public void testThatPipeliningWorksWhenSlowRequestsInDifferentOrder() throws InterruptedException {
        final int numberOfRequests = randomIntBetween(2, 128);
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(
            new Netty4HttpPipeliningHandler(logger, numberOfRequests),
            new WorkEmulatorHandler()
        );

        for (int i = 0; i < numberOfRequests; i++) {
            embeddedChannel.writeInbound(createHttpRequest("/" + String.valueOf(i)));
        }

        // random order execution
        final List<String> urls = new ArrayList<>(waitingRequests.keySet());
        Randomness.shuffle(urls);
        final List<CountDownLatch> latches = new ArrayList<>();
        for (final String url : urls) {
            latches.add(finishRequest(url));
        }

        for (final CountDownLatch latch : latches) {
            latch.await();
        }

        embeddedChannel.flush();

        for (int i = 0; i < numberOfRequests; i++) {
            assertReadHttpMessageHasContent(embeddedChannel, String.valueOf(i));
        }

        assertTrue(embeddedChannel.isOpen());
    }

    public void testThatPipeliningClosesConnectionWithTooManyEvents() throws InterruptedException {
        final int numberOfRequests = randomIntBetween(2, 128);
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(
            new Netty4HttpPipeliningHandler(logger, numberOfRequests),
            new WorkEmulatorHandler()
        );

        for (int i = 0; i < 1 + numberOfRequests + 1; i++) {
            embeddedChannel.writeInbound(createHttpRequest("/" + Integer.toString(i)));
        }

        final List<CountDownLatch> latches = new ArrayList<>();
        final List<Integer> requests = IntStream.range(1, numberOfRequests + 1).boxed().collect(Collectors.toList());
        Randomness.shuffle(requests);

        for (final Integer request : requests) {
            latches.add(finishRequest(request.toString()));
        }

        for (final CountDownLatch latch : latches) {
            latch.await();
        }

        finishRequest(Integer.toString(numberOfRequests + 1)).await();

        embeddedChannel.flush();

        assertFalse(embeddedChannel.isOpen());
    }

    private enum ClosePath {
        OUTBOUND,
        TRANSPORT,
        UPSTREAM,
        HANDLER_REMOVAL,
        CONSUMED_INACTIVE
    }

    public void testQueuedResponsesAreReleasedAtEveryClosePath() {
        for (ClosePath path : ClosePath.values()) {
            assertQueuedResponsesAreReleased(path);
        }
    }

    private void assertQueuedResponsesAreReleased(ClosePath path) {
        final Netty4HttpPipeliningHandler handler = new Netty4HttpPipeliningHandler(logger, 3);
        final EmbeddedChannel channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelInactive(ChannelHandlerContext ctx) {
                if (path != ClosePath.CONSUMED_INACTIVE) {
                    ctx.fireChannelInactive();
                }
            }
        }, handler);
        final Netty4HttpResponse[] responses = { newResponse(), newResponse() };
        final List<ChannelPromise> promises = new ArrayList<>();
        final AtomicInteger releasedAtCompletion = new AtomicInteger();
        try {
            for (int i = 0; i < responses.length; i++) {
                final Netty4HttpResponse response = responses[i];
                final ChannelPromise promise = channel.newPromise();
                promise.addListener(future -> {
                    if (response.refCnt() == 0) {
                        releasedAtCompletion.incrementAndGet();
                    }
                });
                promises.add(promise);
                // Both generic and transport-specific wrappers must release their delegates. Sequence zero stays outstanding.
                final HttpPipelinedResponse pending = i == 0
                    ? new HttpPipelinedResponse(i + 1, response)
                    : new Netty4HttpPipelinedResponse(i + 1, response);
                channel.writeAndFlush(pending, promise);
                assertFalse(path.toString(), promise.isDone());
            }
            switch (path) {
                case OUTBOUND -> channel.close();
                case TRANSPORT, CONSUMED_INACTIVE -> channel.unsafe().close(channel.newPromise());
                case UPSTREAM -> channel.pipeline().context(handler).close();
                case HANDLER_REMOVAL -> channel.pipeline().remove(handler);
            }
            channel.runPendingTasks();
            assertNull(path.toString(), channel.pipeline().context(handler));
            assertEquals(path.toString(), path == ClosePath.HANDLER_REMOVAL, channel.isOpen());
            for (int i = 0; i < responses.length; i++) {
                assertTrue(path.toString(), promises.get(i).cause() instanceof ClosedChannelException);
                assertEquals(path.toString(), 0, responses[i].refCnt());
            }
            assertEquals(path + " must release buffers before completing promises", responses.length, releasedAtCompletion.get());
            channel.close();
            channel.runPendingTasks();
            assertEquals(path + " must complete each response once", responses.length, releasedAtCompletion.get());
        } finally {
            channel.finishAndReleaseAll();
            releaseIfNeeded(responses);
        }
    }

    public void testPipeliningResponseAfterTransportCloseIsRejected() {
        final EmbeddedChannel channel = new EmbeddedChannel(new Netty4HttpPipeliningHandler(logger, 3));
        final Netty4HttpResponse response = newResponse();
        final ChannelPromise promise = channel.newPromise();
        channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelInactive(ChannelHandlerContext ctx) {
                // An asynchronous response can arrive after closure but before pipeline teardown drains the queue.
                ctx.writeAndFlush(new Netty4HttpPipelinedResponse(1, response), promise);
                ctx.fireChannelInactive();
            }
        });
        try {
            channel.unsafe().close(channel.newPromise());
            channel.runPendingTasks();
            assertTrue("A late response must not be queued on a closed channel", promise.isDone());
            assertTrue(promise.cause() instanceof ClosedChannelException);
            assertEquals(0, response.refCnt());
        } finally {
            channel.finishAndReleaseAll();
            releaseIfNeeded(response);
        }
    }

    public void testResponseBufferAccountingIsReleasedOnTransportClose() throws IOException {
        final Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        final EmbeddedChannel channel = new EmbeddedChannel(new Netty4HttpPipeliningHandler(logger, 3));
        try (
            HierarchyCircuitBreakerService breakerService = new HierarchyCircuitBreakerService(
                settings,
                List.of(),
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            );
            ReleasableBytesStreamOutput output = new ReleasableBytesStreamOutput(
                new BigArrays(null, breakerService, CircuitBreaker.REQUEST).withCircuitBreaking()
            )
        ) {
            output.writeByte((byte) 1);
            final CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
            assertTrue(breaker.getUsed() > 0);

            // DefaultRestChannel also releases its response output from the HTTP completion listener.
            // The try-with-resources close is an idempotent fallback if an assertion fails.
            new Netty4HttpChannel(channel).sendResponse(
                new Netty4HttpPipelinedResponse(
                    1,
                    new Netty4HttpResponse(EmptyHttpHeaders.INSTANCE, HTTP_1_1, RestStatus.OK, output.bytes())
                ),
                ActionListener.wrap(output::close)
            );
            assertTrue("The response is held behind sequence zero", breaker.getUsed() > 0);

            channel.unsafe().close(channel.newPromise());
            channel.runPendingTasks();
            assertEquals("Disconnected responses must return their buffer reservation", 0, breaker.getUsed());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    public void testReentrantResponseDuringCloseIsRejected() {
        final EmbeddedChannel channel = new EmbeddedChannel(new Netty4HttpPipeliningHandler(logger, 3));
        final Netty4HttpResponse first = newResponse();
        final Netty4HttpResponse reentrant = newResponse();
        final ChannelPromise firstPromise = channel.newPromise();
        final ChannelPromise reentrantPromise = channel.newPromise();
        firstPromise.addListener(future -> channel.writeAndFlush(new Netty4HttpPipelinedResponse(2, reentrant), reentrantPromise));
        try {
            channel.writeAndFlush(new Netty4HttpPipelinedResponse(1, first), firstPromise);
            channel.close();
            channel.runPendingTasks();
            assertTrue(firstPromise.cause() instanceof ClosedChannelException);
            assertTrue(reentrantPromise.cause() instanceof ClosedChannelException);
            assertEquals("Queued buffers must be released before completing their promises", 0, first.refCnt());
            assertEquals(0, reentrant.refCnt());
        } finally {
            channel.finishAndReleaseAll();
            releaseIfNeeded(first);
            releaseIfNeeded(reentrant);
        }
    }

    public void testCompletionCannotCancelAnOwnedResponse() {
        final EmbeddedChannel channel = new EmbeddedChannel(new Netty4HttpPipeliningHandler(logger, 3));
        final Netty4HttpResponse response = newResponse();
        final ChannelPromise promise = channel.newPromise();
        try {
            channel.writeAndFlush(new Netty4HttpPipelinedResponse(1, response), promise);
            assertFalse("Cancellation must not release buffers still owned by the queue", promise.cancel(false));
            assertFalse(promise.isDone());
            assertEquals(1, response.refCnt());
            channel.close();
            assertEquals(0, response.refCnt());
            assertTrue(promise.cause() instanceof ClosedChannelException);
        } finally {
            channel.finishAndReleaseAll();
            releaseIfNeeded(response);
        }
    }

    public void testReadyResponsesDrainInOrder() {
        assertReadyResponsesDrainInOrder(false, true); // ready response drains a full queue
        assertReadyResponsesDrainInOrder(true, false); // reentrant write cannot overtake the active handoff
        assertReadyResponsesDrainInOrder(true, true);  // reentrant ready response drains a full queue
    }

    private void assertReadyResponsesDrainInOrder(boolean reentrant, boolean fullQueue) {
        final Netty4HttpResponse[] responses = { newResponse(), newResponse(), newResponse() };
        final int count = reentrant && fullQueue ? 3 : 2;
        final EmbeddedChannel channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                if (reentrant && msg == responses[0]) {
                    ctx.channel().write(new Netty4HttpPipelinedResponse(1, responses[1]));
                }
                ctx.write(msg, promise);
            }
        }, new Netty4HttpPipeliningHandler(logger, fullQueue ? 1 : 3));
        try {
            if (fullQueue) {
                channel.writeAndFlush(new Netty4HttpPipelinedResponse(count - 1, responses[count - 1]));
            }
            channel.writeAndFlush(new Netty4HttpPipelinedResponse(0, responses[0]));
            assertTrue("A ready response must be allowed to drain the queue", channel.isActive());
            for (int i = 0; i < count; i++) {
                assertSame("Response " + i + " must remain in sequence", responses[i], channel.readOutbound());
            }
            assertNull(channel.readOutbound());
        } finally {
            channel.finishAndReleaseAll();
            releaseIfNeeded(responses);
        }
    }

    public void testOverflowReleasesAllOwnedResponses() {
        final EmbeddedChannel channel = new EmbeddedChannel(new Netty4HttpPipeliningHandler(logger, 1));
        final Netty4HttpResponse first = newResponse();
        final Netty4HttpResponse second = newResponse();
        final ChannelPromise firstPromise = channel.newPromise();
        final ChannelPromise secondPromise = channel.newPromise();
        try {
            channel.writeAndFlush(new Netty4HttpPipelinedResponse(1, first), firstPromise);
            channel.writeAndFlush(new Netty4HttpPipelinedResponse(2, second), secondPromise);
            assertFalse(channel.isOpen());
            assertTrue(firstPromise.cause() instanceof ClosedChannelException);
            assertTrue(secondPromise.cause() instanceof ClosedChannelException);
            assertEquals(0, first.refCnt());
            assertEquals(0, second.refCnt());
        } finally {
            channel.finishAndReleaseAll();
            releaseIfNeeded(first);
            releaseIfNeeded(second);
        }
    }

    public void testHandoffTransfersBufferOwnershipToNetty() {
        final EmbeddedChannel channel = new EmbeddedChannel(new Netty4HttpPipeliningHandler(logger, 3));
        final Netty4HttpResponse response = newResponse();
        final ChannelPromise promise = channel.newPromise();
        try {
            channel.writeAndFlush(new Netty4HttpPipelinedResponse(0, response), promise);
            assertTrue(promise.isSuccess());
            channel.close();
            assertEquals("The outbound transport still owns the written buffer", 1, response.refCnt());
            assertSame(response, channel.readOutbound());
        } finally {
            channel.outboundMessages().clear();
            channel.finishAndReleaseAll();
            releaseIfNeeded(response);
        }
    }

    public void testNonPositiveLimitRejectsResponses() {
        for (int limit : new int[] { 0, -1 }) {
            final EmbeddedChannel channel = new EmbeddedChannel(new Netty4HttpPipeliningHandler(logger, limit));
            final Netty4HttpResponse response = newResponse();
            final ChannelPromise promise = channel.newPromise();
            try {
                channel.writeAndFlush(new Netty4HttpPipelinedResponse(0, response), promise);
                assertFalse(channel.isOpen());
                assertTrue(promise.cause() instanceof ClosedChannelException);
                assertEquals(0, response.refCnt());
            } finally {
                channel.finishAndReleaseAll();
                releaseIfNeeded(response);
            }
        }
    }

    public void testRemovalDuringHandoffReleasesOnlyQueuedResponses() {
        final Netty4HttpPipeliningHandler handler = new Netty4HttpPipeliningHandler(logger, 3);
        final Netty4HttpResponse first = newResponse();
        final Netty4HttpResponse second = newResponse();
        final EmbeddedChannel channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                ctx.pipeline().remove(handler);
                ctx.write(msg, promise);
            }
        }, handler);
        final ChannelPromise secondPromise = channel.newPromise();
        try {
            channel.writeAndFlush(new Netty4HttpPipelinedResponse(1, second), secondPromise);
            channel.writeAndFlush(new Netty4HttpPipelinedResponse(0, first));
            assertTrue(secondPromise.cause() instanceof ClosedChannelException);
            assertEquals(0, second.refCnt());
            assertEquals(1, first.refCnt());
            assertSame(first, channel.readOutbound());
            assertNull(channel.readOutbound());
        } finally {
            channel.outboundMessages().clear();
            channel.finishAndReleaseAll();
            releaseIfNeeded(first);
            releaseIfNeeded(second);
        }
    }

    public void testCancellationClosesConnectionAndRejectsLaterResponses() {
        assertCancelledResponseClosesConnection(false, false);
        assertCancelledResponseClosesConnection(true, false);
        assertCancelledResponseClosesConnection(false, true);
    }

    private void assertCancelledResponseClosesConnection(boolean outOfOrder, boolean deferClose) {
        final Netty4HttpPipeliningHandler handler = new Netty4HttpPipeliningHandler(logger, 3);
        final AtomicReference<Runnable> finishClose = new AtomicReference<>();
        final EmbeddedChannel channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            @Override
            public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
                if (deferClose) {
                    finishClose.set(() -> ctx.close(promise));
                } else {
                    ctx.close(promise);
                }
            }
        }, handler);
        final Netty4HttpResponse cancelled = newResponse();
        final Netty4HttpResponse queued = newResponse();
        final Netty4HttpResponse late = newResponse();
        final ChannelPromise cancelledPromise = channel.newPromise();
        final ChannelPromise queuedPromise = channel.newPromise();
        final ChannelPromise latePromise = channel.newPromise();
        try {
            channel.writeAndFlush(new Netty4HttpPipelinedResponse(outOfOrder ? 2 : 1, queued), queuedPromise);
            assertFalse(queuedPromise.isDone());
            assertTrue(cancelledPromise.cancel(false));
            // Model cancellation after Netty validates an off-loop write but before its task reaches this handler.
            // A write cancelled before validation is discarded by Netty without invoking the handler.
            handler.write(
                channel.pipeline().context(handler),
                new Netty4HttpPipelinedResponse(outOfOrder ? 1 : 0, cancelled),
                cancelledPromise
            );
            if (deferClose) {
                channel.writeAndFlush(new Netty4HttpPipelinedResponse(2, late), latePromise);
                assertTrue(latePromise.cause() instanceof ClosedChannelException);
                assertEquals(0, late.refCnt());
            } else {
                channel.flush();
            }
            assertNull("No response may be delivered for a different request after cancellation", channel.readOutbound());
            assertTrue(cancelledPromise.isCancelled());
            assertTrue(queuedPromise.cause() instanceof ClosedChannelException);
            assertEquals(0, cancelled.refCnt());
            assertEquals(0, queued.refCnt());
            if (deferClose) {
                assertTrue("The transport remains active until the earlier handler completes close", channel.isActive());
                assertNotNull(finishClose.get());
                finishClose.getAndSet(null).run();
            }
            assertFalse(channel.isOpen());
        } finally {
            channel.close();
            if (finishClose.get() != null) {
                finishClose.getAndSet(null).run();
            }
            channel.finishAndReleaseAll();
            releaseIfNeeded(cancelled, queued, late);
        }
    }

    public void testResponsesAreRejectedWhileTransportCloseIsDeferred() {
        final AtomicReference<Runnable> finishClose = new AtomicReference<>();
        final EmbeddedChannel channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            @Override
            public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
                finishClose.set(() -> ctx.close(promise));
            }
        }, new Netty4HttpPipeliningHandler(logger, 3));
        final Netty4HttpResponse queued = newResponse();
        final Netty4HttpResponse late = newResponse();
        final ChannelPromise queuedPromise = channel.newPromise();
        final ChannelPromise latePromise = channel.newPromise();
        try {
            channel.writeAndFlush(new Netty4HttpPipelinedResponse(1, queued), queuedPromise);
            channel.close();
            assertTrue("The earlier handler is still deferring transport closure", channel.isActive());
            assertTrue(queuedPromise.cause() instanceof ClosedChannelException);
            assertEquals(0, queued.refCnt());
            channel.writeAndFlush(new Netty4HttpPipelinedResponse(2, late), latePromise);
            assertTrue(latePromise.cause() instanceof ClosedChannelException);
            assertEquals(0, late.refCnt());
        } finally {
            channel.close();
            if (finishClose.get() != null) {
                finishClose.get().run();
            }
            channel.finishAndReleaseAll();
            releaseIfNeeded(queued, late);
        }
    }

    private static void releaseIfNeeded(ReferenceCounted... responses) {
        for (ReferenceCounted response : responses) {
            if (response.refCnt() > 0) {
                response.release();
            }
        }
    }

    private static Netty4HttpResponse newResponse() {
        return new Netty4HttpResponse(EmptyHttpHeaders.INSTANCE, HTTP_1_1, RestStatus.OK, new BytesArray("response"));
    }

    private void assertReadHttpMessageHasContent(EmbeddedChannel embeddedChannel, String expectedContent) {
        FullHttpResponse response = (FullHttpResponse) embeddedChannel.outboundMessages().poll();
        assertNotNull("Expected response to exist, maybe you did not wait long enough?", response);
        assertNotNull("Expected response to have content " + expectedContent, response.content());
        String data = new String(ByteBufUtil.getBytes(response.content()), StandardCharsets.UTF_8);
        assertThat(data, is(expectedContent));
    }

    private Netty4HttpRequest createHttpRequest(String uri) {
        return new Netty4HttpRequest(new DefaultFullHttpRequest(HTTP_1_1, HttpMethod.GET, uri), HttpResponseHeadersFactories.newDefault());
    }

    private class WorkEmulatorHandler extends SimpleChannelInboundHandler<HttpPipelinedRequest> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, HttpPipelinedRequest pipelinedRequest) {
            final org.opensearch.http.HttpRequest request = pipelinedRequest.getDelegateRequest();
            final QueryStringDecoder decoder = new QueryStringDecoder(request.uri());

            final String uri = decoder.path().replace("/", "");
            final BytesReference content = new BytesArray(uri.getBytes(StandardCharsets.UTF_8));
            HttpResponse httpResponse = pipelinedRequest.createResponse(RestStatus.OK, content);
            httpResponse.addHeader(CONTENT_LENGTH.toString(), Integer.toString(content.length()));

            final CountDownLatch waitingLatch = new CountDownLatch(1);
            waitingRequests.put(uri, waitingLatch);
            final CountDownLatch finishingLatch = new CountDownLatch(1);
            finishingRequests.put(uri, finishingLatch);

            handlerService.submit(() -> {
                try {
                    waitingLatch.await(1000, TimeUnit.SECONDS);
                    final ChannelPromise promise = ctx.newPromise();
                    eventLoopService.submit(() -> {
                        ctx.write(httpResponse, promise);
                        finishingLatch.countDown();
                    });
                } catch (InterruptedException e) {
                    fail(e.toString());
                }
            });
        }
    }
}

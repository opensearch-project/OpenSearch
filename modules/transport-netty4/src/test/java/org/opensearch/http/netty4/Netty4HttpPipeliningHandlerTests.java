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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipelineException;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.ReferenceCounted;

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

    public void testPipeliningRequestsAreReleased() throws InterruptedException {
        final int numberOfRequests = 10;
        final EmbeddedChannel embeddedChannel = new EmbeddedChannel(new Netty4HttpPipeliningHandler(logger, numberOfRequests + 1));

        for (int i = 0; i < numberOfRequests; i++) {
            embeddedChannel.writeInbound(createHttpRequest("/" + i));
        }

        HttpPipelinedRequest inbound;
        ArrayList<HttpPipelinedRequest> requests = new ArrayList<>();
        while ((inbound = embeddedChannel.readInbound()) != null) {
            requests.add(inbound);
        }

        ArrayList<ChannelPromise> promises = new ArrayList<>();
        for (int i = 1; i < requests.size(); ++i) {
            ChannelPromise promise = embeddedChannel.newPromise();
            promises.add(promise);
            HttpPipelinedRequest pipelinedRequest = requests.get(i);
            HttpPipelinedResponse resp = pipelinedRequest.createResponse(RestStatus.OK, BytesArray.EMPTY);
            embeddedChannel.writeAndFlush(resp, promise);
        }

        for (ChannelPromise promise : promises) {
            assertFalse(promise.isDone());
        }
        embeddedChannel.close().syncUninterruptibly();
        for (ChannelPromise promise : promises) {
            assertTrue(promise.isDone());
            assertTrue(promise.cause() instanceof ClosedChannelException);
        }
    }

    public void testPipeliningResponsesAreReleasedOnTransportClose() {
        // A peer disconnect closes the transport without traversing outbound close handlers.
        assertPipeliningResponsesAreReleased(channel -> channel.unsafe().close(channel.newPromise()));
    }

    public void testPipeliningResponsesAreReleasedOnUpstreamClose() {
        // A decoder or timeout handler can close from a context before the pipelining handler.
        assertPipeliningResponsesAreReleased(channel -> channel.pipeline().context(Netty4HttpPipeliningHandler.class).close());
    }

    public void testPipeliningResponseAfterTransportCloseIsRejected() {
        final EmbeddedChannel channel = new EmbeddedChannel(new Netty4HttpPipeliningHandler(logger, 3));
        final ChannelPromise promise = channel.newPromise();
        channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelInactive(ChannelHandlerContext ctx) {
                // An asynchronous response can arrive after the holding queue has been drained.
                ctx.writeAndFlush(
                    new HttpPipelinedResponse(
                        1,
                        new Netty4HttpResponse(EmptyHttpHeaders.INSTANCE, HTTP_1_1, RestStatus.OK, BytesArray.EMPTY)
                    ),
                    promise
                );
                ctx.fireChannelInactive();
            }
        });
        try {
            channel.unsafe().close(channel.newPromise());
            channel.runPendingTasks();
            assertTrue("A late response must not be queued on a closed channel", promise.isDone());
            assertTrue(promise.cause() instanceof ClosedChannelException);
        } finally {
            channel.finishAndReleaseAll();
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
                new HttpPipelinedResponse(1, new Netty4HttpResponse(EmptyHttpHeaders.INSTANCE, HTTP_1_1, RestStatus.OK, output.bytes())),
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

    public void testPendingResponseIsReleasedOnHandlerRemoval() {
        final Netty4HttpPipeliningHandler handler = new Netty4HttpPipeliningHandler(logger, 3);
        final EmbeddedChannel channel = new EmbeddedChannel(handler);
        final Netty4HttpResponse response = newResponse();
        final ChannelPromise promise = channel.newPromise();
        try {
            channel.writeAndFlush(new HttpPipelinedResponse(1, response), promise);
            assertFalse(promise.isDone());
            channel.pipeline().remove(handler);
            assertTrue("Removing the owner must complete pending responses", promise.isDone());
            assertTrue(promise.cause() instanceof ClosedChannelException);
            assertEquals(0, response.refCnt());
        } finally {
            channel.finishAndReleaseAll();
            releaseIfNeeded(response);
        }
    }

    public void testHandlerRemovalAfterInitializationFailure() {
        final IllegalStateException initializationFailure = new IllegalStateException("close future unavailable");
        final AtomicBoolean failInitialization = new AtomicBoolean();
        final List<Throwable> failures = new ArrayList<>();
        final EmbeddedChannel channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter() {
            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                failures.add(cause);
            }
        }) {
            @Override
            public ChannelFuture closeFuture() {
                if (failInitialization.getAndSet(false)) {
                    throw initializationFailure;
                }
                return super.closeFuture();
            }
        };
        final Netty4HttpPipeliningHandler handler = new Netty4HttpPipeliningHandler(logger, 3);
        try {
            failInitialization.set(true);
            channel.pipeline().addLast(handler);
            assertNull(channel.pipeline().context(handler));
            assertEquals(1, failures.size());
            assertTrue(failures.get(0) instanceof ChannelPipelineException);
            assertSame(initializationFailure, failures.get(0).getCause());
            assertTrue(
                "Netty must be able to remove the partially initialized handler",
                failures.get(0).getMessage().endsWith("; removed.")
            );
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    public void testCloseFutureReleasesResponsesWhenInactiveIsSwallowed() {
        final EmbeddedChannel channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelInactive(ChannelHandlerContext ctx) {
                // A preceding handler does not propagate this event.
            }
        }, new Netty4HttpPipeliningHandler(logger, 3));
        final Netty4HttpResponse response = newResponse();
        final ChannelPromise promise = channel.newPromise();
        try {
            channel.writeAndFlush(new HttpPipelinedResponse(1, response), promise);
            AtomicBoolean releasedAtClose = new AtomicBoolean();
            channel.closeFuture().addListener(future -> releasedAtClose.set(promise.isDone() && response.refCnt() == 0));
            channel.unsafe().close(channel.newPromise());
            // The close future completes before channelInactive and deferred handler removal can perform cleanup.
            assertTrue("Responses must be released when the transport closes, before pipeline teardown", releasedAtClose.get());
            assertTrue("The owner must be bound to transport closure", promise.isDone());
            assertTrue(promise.cause() instanceof ClosedChannelException);
            assertEquals(0, response.refCnt());
        } finally {
            channel.finishAndReleaseAll();
            releaseIfNeeded(response);
        }
    }

    public void testReentrantResponseDuringCloseIsRejected() {
        final EmbeddedChannel channel = new EmbeddedChannel(new Netty4HttpPipeliningHandler(logger, 3));
        final Netty4HttpResponse first = newResponse();
        final Netty4HttpResponse reentrant = newResponse();
        final ChannelPromise firstPromise = channel.newPromise();
        final ChannelPromise reentrantPromise = channel.newPromise();
        firstPromise.addListener(future -> channel.writeAndFlush(new HttpPipelinedResponse(2, reentrant), reentrantPromise));
        try {
            channel.writeAndFlush(new HttpPipelinedResponse(1, first), firstPromise);
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
            channel.writeAndFlush(new HttpPipelinedResponse(1, response), promise);
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

    public void testReentrantWritesPreserveResponseOrder() {
        final Netty4HttpResponse first = newResponse();
        final Netty4HttpResponse second = newResponse();
        final EmbeddedChannel channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                if (msg == first) {
                    ctx.channel().write(new HttpPipelinedResponse(1, second));
                }
                ctx.write(msg, promise);
            }
        }, new Netty4HttpPipeliningHandler(logger, 3));
        try {
            channel.writeAndFlush(new HttpPipelinedResponse(0, first));
            assertSame(first, channel.outboundMessages().poll());
            assertSame(second, channel.outboundMessages().poll());
            assertNull(channel.readOutbound());
        } finally {
            channel.outboundMessages().clear();
            channel.finishAndReleaseAll();
            releaseIfNeeded(first, second);
        }
    }

    public void testReadyResponseCanDrainAFullQueue() {
        final EmbeddedChannel channel = new EmbeddedChannel(new Netty4HttpPipeliningHandler(logger, 1));
        final Netty4HttpResponse first = newResponse();
        final Netty4HttpResponse second = newResponse();
        try {
            channel.writeAndFlush(new HttpPipelinedResponse(1, second));
            channel.writeAndFlush(new HttpPipelinedResponse(0, first));
            assertTrue("A ready response does not consume a holding slot", channel.isActive());
            assertSame(first, channel.outboundMessages().poll());
            assertSame(second, channel.outboundMessages().poll());
        } finally {
            channel.outboundMessages().clear();
            channel.finishAndReleaseAll();
            releaseIfNeeded(first);
            releaseIfNeeded(second);
        }
    }

    public void testOverflowReleasesAllOwnedResponses() {
        final EmbeddedChannel channel = new EmbeddedChannel(new Netty4HttpPipeliningHandler(logger, 1));
        final Netty4HttpResponse first = newResponse();
        final Netty4HttpResponse second = newResponse();
        final ChannelPromise firstPromise = channel.newPromise();
        final ChannelPromise secondPromise = channel.newPromise();
        try {
            channel.writeAndFlush(new HttpPipelinedResponse(1, first), firstPromise);
            channel.writeAndFlush(new HttpPipelinedResponse(2, second), secondPromise);
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

    public void testStaleResponseClosesConnectionAndReleasesQueuedResponses() {
        final EmbeddedChannel channel = new EmbeddedChannel(new Netty4HttpPipeliningHandler(logger, 3));
        final Netty4HttpResponse first = newResponse();
        final Netty4HttpResponse queued = newResponse();
        final Netty4HttpResponse stale = newResponse();
        final ChannelPromise queuedPromise = channel.newPromise();
        final ChannelPromise stalePromise = channel.newPromise();
        try {
            assertTrue(channel.writeAndFlush(new HttpPipelinedResponse(0, first)).isSuccess());
            assertSame(first, channel.readOutbound());
            first.release();

            channel.writeAndFlush(new HttpPipelinedResponse(2, queued), queuedPromise);
            assertFalse(queuedPromise.isDone());
            channel.writeAndFlush(new HttpPipelinedResponse(0, stale), stalePromise);

            assertFalse("A stale response must close the connection instead of blocking the queue", channel.isOpen());
            assertTrue(queuedPromise.cause() instanceof ClosedChannelException);
            assertTrue(stalePromise.cause() instanceof ClosedChannelException);
            assertEquals(0, queued.refCnt());
            assertEquals(0, stale.refCnt());
            assertNull(channel.readOutbound());
        } finally {
            channel.finishAndReleaseAll();
            releaseIfNeeded(first, queued, stale);
        }
    }

    public void testDuplicateQueuedSequenceClosesConnection() {
        final EmbeddedChannel channel = new EmbeddedChannel(new Netty4HttpPipeliningHandler(logger, 3));
        final Netty4HttpResponse first = newResponse();
        final Netty4HttpResponse second = newResponse();
        final Netty4HttpResponse duplicate = newResponse();
        final Netty4HttpResponse later = newResponse();
        try {
            final ChannelFuture secondWrite = channel.writeAndFlush(new HttpPipelinedResponse(1, second));
            final ChannelFuture duplicateWrite = channel.writeAndFlush(new HttpPipelinedResponse(1, duplicate));
            final ChannelFuture laterWrite = channel.writeAndFlush(new HttpPipelinedResponse(2, later));
            assertFalse(secondWrite.isDone());
            assertFalse(duplicateWrite.isDone());
            assertFalse(laterWrite.isDone());
            channel.writeAndFlush(new HttpPipelinedResponse(0, first));

            assertFalse("A duplicate already in the queue must not block every later response", channel.isOpen());
            assertTrue(secondWrite.isDone());
            assertTrue(duplicateWrite.isDone());
            assertTrue(duplicateWrite.cause() instanceof ClosedChannelException);
            assertEquals(0, duplicate.refCnt());
            assertTrue(laterWrite.cause() instanceof ClosedChannelException);
            assertEquals(0, later.refCnt());
        } finally {
            channel.finishAndReleaseAll();
            releaseIfNeeded(first, second, duplicate, later);
        }
    }

    public void testHandoffTransfersBufferOwnershipToNetty() {
        final EmbeddedChannel channel = new EmbeddedChannel(new Netty4HttpPipeliningHandler(logger, 3));
        final Netty4HttpResponse response = newResponse();
        final ChannelPromise promise = channel.newPromise();
        try {
            channel.writeAndFlush(new HttpPipelinedResponse(0, response), promise);
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
                channel.writeAndFlush(new HttpPipelinedResponse(0, response), promise);
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
            channel.writeAndFlush(new HttpPipelinedResponse(1, second), secondPromise);
            channel.writeAndFlush(new HttpPipelinedResponse(0, first));
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

    public void testCancelledResponseClosesConnection() {
        assertCancelledResponseClosesConnection(false, false);
    }

    public void testCancelledOutOfOrderResponseClosesConnection() {
        assertCancelledResponseClosesConnection(true, false);
    }

    public void testCancelledResponseRejectsWritesWhileCloseIsDeferred() {
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
            channel.writeAndFlush(new HttpPipelinedResponse(outOfOrder ? 2 : 1, queued), queuedPromise);
            assertFalse(queuedPromise.isDone());
            assertTrue(cancelledPromise.cancel(false));
            // Model cancellation after Netty validates an off-loop write but before its task reaches this handler.
            // A write cancelled before validation is discarded by Netty without invoking the handler.
            handler.write(channel.pipeline().context(handler), new HttpPipelinedResponse(outOfOrder ? 1 : 0, cancelled), cancelledPromise);
            if (deferClose) {
                channel.writeAndFlush(new HttpPipelinedResponse(2, late), latePromise);
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

    public void testReentrantReadyResponseCanDrainAFullQueue() {
        final Netty4HttpResponse first = newResponse();
        final Netty4HttpResponse second = newResponse();
        final Netty4HttpResponse third = newResponse();
        final EmbeddedChannel channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                if (msg == first) {
                    ctx.channel().write(new HttpPipelinedResponse(1, second));
                }
                ctx.write(msg, promise);
            }
        }, new Netty4HttpPipeliningHandler(logger, 1));
        try {
            channel.writeAndFlush(new HttpPipelinedResponse(2, third));
            channel.writeAndFlush(new HttpPipelinedResponse(0, first));
            assertTrue("The ready reentrant response must be allowed to drain the full queue", channel.isActive());
            assertSame(first, channel.readOutbound());
            assertSame(second, channel.readOutbound());
            assertSame(third, channel.readOutbound());
            assertNull(channel.readOutbound());
        } finally {
            channel.finishAndReleaseAll();
            releaseIfNeeded(first, second, third);
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
            channel.writeAndFlush(new HttpPipelinedResponse(1, queued), queuedPromise);
            channel.close();
            assertTrue("The earlier handler is still deferring transport closure", channel.isActive());
            assertTrue(queuedPromise.cause() instanceof ClosedChannelException);
            assertEquals(0, queued.refCnt());
            channel.writeAndFlush(new HttpPipelinedResponse(2, late), latePromise);
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

    private void assertPipeliningResponsesAreReleased(Consumer<EmbeddedChannel> closeChannel) {
        final EmbeddedChannel channel = new EmbeddedChannel(new Netty4HttpPipeliningHandler(logger, 3));
        final List<HttpPipelinedRequest> requests = new ArrayList<>();
        final List<ChannelPromise> promises = new ArrayList<>();
        final AtomicInteger completions = new AtomicInteger();
        try {
            for (int i = 0; i < 3; i++) {
                channel.writeInbound(createHttpRequest("/" + i));
                requests.add(channel.readInbound());
            }

            // Keep the first response outstanding so later responses remain in the holding queue.
            for (int i = 1; i < requests.size(); i++) {
                ChannelPromise promise = channel.newPromise();
                promise.addListener(future -> completions.incrementAndGet());
                promises.add(promise);
                channel.writeAndFlush(requests.get(i).createResponse(RestStatus.OK, BytesArray.EMPTY), promise);
                assertFalse(promise.isDone());
            }

            closeChannel.accept(channel);
            channel.runPendingTasks();
            assertFalse(channel.isOpen());
            assertEquals("Queued response completion listeners must run on disconnect", promises.size(), completions.get());
            for (ChannelPromise promise : promises) {
                assertTrue(promise.isDone());
                assertTrue(promise.cause() instanceof ClosedChannelException);
            }
            channel.close();
            channel.runPendingTasks();
            assertEquals("Each response must complete exactly once", promises.size(), completions.get());
        } finally {
            requests.forEach(HttpPipelinedRequest::release);
            channel.finishAndReleaseAll();
        }
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

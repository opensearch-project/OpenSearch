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

import org.apache.logging.log4j.Logger;
import org.opensearch.common.collect.Tuple;
import org.opensearch.http.HttpPipelinedRequest;
import org.opensearch.http.HttpPipelinedResponse;
import org.opensearch.http.HttpPipeliningAggregator;

import java.nio.channels.ClosedChannelException;
import java.util.List;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their corresponding requests.
 */
public class Netty4HttpPipeliningHandler extends ChannelDuplexHandler {

    private final Logger logger;
    private final HttpPipeliningAggregator<ChannelPromise> aggregator;
    private PendingResponses pendingResponses;

    /**
     * Construct a new pipelining handler; this handler should be used downstream of HTTP decoding/aggregation.
     *
     * @param logger        for logging unexpected errors
     * @param maxEventsHeld the maximum number of channel events that will be retained prior to aborting the channel connection; this is
     *                      required as events cannot queue up indefinitely
     */
    public Netty4HttpPipeliningHandler(Logger logger, final int maxEventsHeld) {
        this.logger = logger;
        this.aggregator = new HttpPipeliningAggregator<>(maxEventsHeld);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        pendingResponses = new PendingResponses(ctx, logger, aggregator);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        assert msg instanceof Netty4HttpRequest : "Invalid message type: " + msg.getClass();
        HttpPipelinedRequest pipelinedRequest = aggregator.read(((Netty4HttpRequest) msg));
        ctx.fireChannelRead(pipelinedRequest);
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
        assert msg instanceof HttpPipelinedResponse : "Invalid message type: " + msg.getClass();
        pendingResponses.write((HttpPipelinedResponse) msg, promise);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
        pendingResponses.close();
        ctx.close(promise);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        // Netty also calls this when handlerAdded fails before assigning pendingResponses.
        if (pendingResponses != null) {
            pendingResponses.close();
        }
    }

    /**
     * Holds responses until they are handed to Netty. The close future releases them before deferred pipeline teardown,
     * even when an earlier handler consumes channelInactive. All state belongs to the context's executor.
     */
    private static final class PendingResponses {
        private final ChannelHandlerContext ctx;
        private final Logger logger;
        private final HttpPipeliningAggregator<ChannelPromise> aggregator;
        private final ChannelFutureListener closeListener;
        // True while the drain loop is on the call stack, not until the network writes complete.
        private boolean draining;
        private boolean closed;

        private PendingResponses(ChannelHandlerContext ctx, Logger logger, HttpPipeliningAggregator<ChannelPromise> aggregator) {
            this.ctx = ctx;
            this.logger = logger;
            this.aggregator = aggregator;
            closeListener = future -> {
                if (ctx.executor().inEventLoop()) {
                    close();
                } else {
                    ctx.executor().execute(this::close);
                }
            };
            ctx.channel().closeFuture().addListener(closeListener);
        }

        private void write(HttpPipelinedResponse response, ChannelPromise promise) {
            assert ctx.executor().inEventLoop();
            // Prevent cancellation from releasing backing storage while queued. If cancellation already won, close:
            // skipping a response would cause HTTP/1.1 clients to associate later responses with the wrong requests.
            if (promise.setUncancellable() == false) {
                rejectAndClose(response, promise);
                return;
            }
            if (closed || ctx.channel().isActive() == false) {
                reject(response, promise, new ClosedChannelException());
                return;
            }
            try {
                aggregator.add(response, promise);
            } catch (IllegalStateException e) {
                logger.debug("closing HTTP connection: {}", e.getMessage());
                rejectAndClose(response, promise);
                return;
            }
            drainReadyResponses();
        }

        private void drainReadyResponses() {
            // A synchronous outbound handler can reenter write() during handoff. The active drain must
            // pick up that queued response so it cannot overtake the response currently being forwarded.
            if (draining) {
                return;
            }
            draining = true;
            try {
                while (closed == false) {
                    final Tuple<HttpPipelinedResponse, ChannelPromise> next;
                    try {
                        next = aggregator.poll();
                    } catch (IllegalStateException e) {
                        logger.debug("closing HTTP connection: {}", e.getMessage());
                        // A duplicate queued response became stale. All remaining responses are still owned by the queue.
                        close();
                        ctx.close();
                        return;
                    }
                    if (next == null) {
                        return;
                    }
                    // Poll one at a time so a reentrant close can still release all responses not yet handed to Netty.
                    ctx.write(next.v1().getDelegateRequest(), next.v2());
                }
            } finally {
                draining = false;
            }
        }

        private void rejectAndClose(HttpPipelinedResponse response, ChannelPromise promise) {
            close();
            // This response was never queued, so close() could not release it.
            reject(response, promise, new ClosedChannelException());
            ctx.close();
        }

        private void close() {
            assert ctx.executor().inEventLoop();
            if (closed) {
                return;
            }
            // Set the terminal state before invoking listeners, which can reenter write() or close().
            closed = true;
            ctx.channel().closeFuture().removeListener(closeListener);
            List<Tuple<HttpPipelinedResponse, ChannelPromise>> pending = aggregator.removeAllInflightResponses();
            if (pending.isEmpty() == false) {
                ClosedChannelException failure = new ClosedChannelException();
                for (Tuple<HttpPipelinedResponse, ChannelPromise> next : pending) {
                    reject(next.v1(), next.v2(), failure);
                }
            }
        }

        private void reject(HttpPipelinedResponse response, ChannelPromise promise, ClosedChannelException failure) {
            try {
                ReferenceCountUtil.release(response.getDelegateRequest());
            } catch (RuntimeException e) {
                logger.error("unexpected error while releasing pipelined http response", e);
            } finally {
                promise.tryFailure(failure);
            }
        }
    }

}

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
import org.opensearch.http.HttpPipelinedRequest;
import org.opensearch.http.HttpPipelinedResponse;

import java.nio.channels.ClosedChannelException;
import java.util.Comparator;
import java.util.PriorityQueue;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their corresponding requests.
 */
public final class Netty4HttpPipeliningHandler extends ChannelDuplexHandler {

    private final Logger logger;
    private final int maxEventsHeld;
    private PendingResponses pendingResponses;
    private int readSequence;

    /**
     * Construct a new pipelining handler; this handler should be used downstream of HTTP decoding/aggregation.
     *
     * @param logger        for logging unexpected errors
     * @param maxEventsHeld the maximum number of channel events that will be retained prior to aborting the channel connection; this is
     *                      required as events cannot queue up indefinitely
     */
    public Netty4HttpPipeliningHandler(Logger logger, final int maxEventsHeld) {
        this.logger = logger;
        this.maxEventsHeld = maxEventsHeld;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        pendingResponses = new PendingResponses(ctx, logger, maxEventsHeld);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        assert msg instanceof Netty4HttpRequest : "Invalid message type: " + msg.getClass();
        HttpPipelinedRequest pipelinedRequest = new HttpPipelinedRequest(readSequence++, (Netty4HttpRequest) msg);
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
    public void channelInactive(ChannelHandlerContext ctx) {
        pendingResponses.close();
        ctx.fireChannelInactive();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        pendingResponses.close();
    }

    /**
     * Owns responses until they are handed to Netty. Construction installs terminal cleanup: callers cannot create an unbound queue,
     * remove entries, or override the enclosing handler's lifecycle. All state belongs to the context's executor.
     */
    private static final class PendingResponses {
        private final ChannelHandlerContext ctx;
        private final Logger logger;
        private final int maxEventsHeld;
        private final ChannelFutureListener closeListener;
        // Allocate holding storage only when a response actually needs to wait.
        private PriorityQueue<PendingResponse> queue;
        private int writeSequence;
        private boolean draining;
        private boolean closed;

        private PendingResponses(ChannelHandlerContext ctx, Logger logger, int maxEventsHeld) {
            this.ctx = ctx;
            this.logger = logger;
            this.maxEventsHeld = maxEventsHeld;
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
            // A completion listener may release the response's backing storage. It must not run while we still own the response.
            if (promise.setUncancellable() == false) {
                ReferenceCountUtil.release(response.getDelegateRequest());
                return;
            }
            if (closed || ctx.channel().isActive() == false) {
                reject(response, promise, new ClosedChannelException());
                return;
            }
            if (maxEventsHeld > 0 && draining == false && response.getSequence() == writeSequence) {
                draining = true;
                try {
                    // The common in-order path allocates neither a queue entry nor a list of ready responses.
                    forward(response, promise);
                    while (closed == false && queue != null && queue.isEmpty() == false) {
                        if (queue.peek().response.getSequence() != writeSequence) {
                            break;
                        }
                        PendingResponse next = queue.poll();
                        forward(next.response, next.promise);
                    }
                } finally {
                    draining = false;
                }
            } else if ((queue == null ? 0 : queue.size()) < maxEventsHeld) {
                if (queue == null) {
                    queue = new PriorityQueue<>(1, Comparator.comparingInt(entry -> entry.response.getSequence()));
                }
                queue.add(new PendingResponse(response, promise));
            } else {
                close();
                reject(response, promise, new ClosedChannelException());
                ctx.close();
            }
        }

        private void forward(HttpPipelinedResponse response, ChannelPromise promise) {
            writeSequence++;
            // Once handed off, Netty owns the buffer and promise. Reentrant writes wait until this handoff returns.
            ctx.write(response.getDelegateRequest(), promise);
        }

        private void close() {
            assert ctx.executor().inEventLoop();
            if (closed) {
                return;
            }
            // Set the terminal state before invoking listeners, which can reenter write() or close().
            closed = true;
            ctx.channel().closeFuture().removeListener(closeListener);
            PriorityQueue<PendingResponse> pending = queue;
            queue = null;
            if (pending != null && pending.isEmpty() == false) {
                ClosedChannelException failure = new ClosedChannelException();
                // Ordering no longer matters: linear traversal avoids sorting responses that will never be written.
                for (PendingResponse next : pending) {
                    reject(next.response, next.promise, failure);
                }
                pending.clear();
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

    private record PendingResponse(HttpPipelinedResponse response, ChannelPromise promise) {
    }
}

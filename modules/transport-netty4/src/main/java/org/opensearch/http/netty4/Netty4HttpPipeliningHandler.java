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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their corresponding requests.
 */
public class Netty4HttpPipeliningHandler extends ChannelDuplexHandler {

    private final Logger logger;
    private final HttpPipeliningAggregator<ChannelPromise> aggregator;
    // True while the drain loop is on the call stack, not until the network writes complete.
    private boolean draining;
    private boolean closed;

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
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        assert msg instanceof Netty4HttpRequest : "Invalid message type: " + msg.getClass();
        HttpPipelinedRequest pipelinedRequest = new Netty4HttpPipelinedRequest(aggregator.nextReadSequence(), (Netty4HttpRequest) msg);
        ctx.fireChannelRead(pipelinedRequest);
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
        assert msg instanceof HttpPipelinedResponse : "Invalid message type: " + msg.getClass();
        assert ctx.executor().inEventLoop();
        final HttpPipelinedResponse response = (HttpPipelinedResponse) msg;
        // Prevent cancellation from releasing backing storage while queued. If cancellation already won, close:
        // skipping a response would cause HTTP/1.1 clients to associate later responses with the wrong requests.
        if (promise.setUncancellable() == false) {
            rejectAndClose(ctx, response, promise);
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
            rejectAndClose(ctx, response, promise);
            return;
        }
        drainReadyResponses(ctx);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
        closePendingResponses(ctx);
        ctx.close(promise);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        // A closed channel's pipeline is destroyed after unregistration, even when an earlier handler consumes
        // channelInactive or initiates closure before this handler.
        closePendingResponses(ctx);
    }

    private void drainReadyResponses(ChannelHandlerContext ctx) {
        // A synchronous outbound handler can reenter write() during handoff. The active drain must
        // pick up that queued response so it cannot overtake the response currently being forwarded.
        if (draining) {
            return;
        }
        draining = true;
        try {
            while (closed == false) {
                final Tuple<HttpPipelinedResponse, ChannelPromise> next = aggregator.poll();
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

    private void rejectAndClose(ChannelHandlerContext ctx, HttpPipelinedResponse response, ChannelPromise promise) {
        closePendingResponses(ctx);
        // This response was never queued, so closePendingResponses() could not release it.
        reject(response, promise, new ClosedChannelException());
        ctx.close();
    }

    private void closePendingResponses(ChannelHandlerContext ctx) {
        assert ctx.executor().inEventLoop();
        if (closed) {
            return;
        }
        // Set the terminal state before invoking listeners, which can reenter write() or close().
        closed = true;
        List<Tuple<HttpPipelinedResponse, ChannelPromise>> pending = aggregator.removeAllInflightResponses();
        if (pending.isEmpty() == false) {
            ClosedChannelException failure = new ClosedChannelException();
            for (Tuple<HttpPipelinedResponse, ChannelPromise> next : pending) {
                reject(next.v1(), next.v2(), failure);
            }
        }
    }

    private static void reject(HttpPipelinedResponse response, ChannelPromise promise, ClosedChannelException failure) {
        ReferenceCountUtil.safeRelease(response.getDelegateRequest());
        promise.tryFailure(failure);
    }
}

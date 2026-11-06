/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.http.HttpPipelinedResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.Netty4NioSocketChannel;
import org.junit.Before;

import java.nio.channels.ClosedChannelException;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ServerChannel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.HttpVersion;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;

public class Netty4HttpChannelTests extends OpenSearchTestCase {
    private Netty4HttpChannel netty4HttpChannel;
    private Channel channel;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        channel = new Netty4NioSocketChannel();
        netty4HttpChannel = new Netty4HttpChannel(channel);
    }

    public void testChannelAttributeMatchesChannel() {
        final Optional<Channel> channelOpt = netty4HttpChannel.get("channel", Channel.class);
        assertThat(channelOpt.isPresent(), is(true));
        assertThat(channelOpt.get(), sameInstance(channel));
    }

    public void testChannelAttributeMatchesChannelOutboundInvoker() {
        final Optional<ChannelOutboundInvoker> channelOpt = netty4HttpChannel.get("channel", ChannelOutboundInvoker.class);
        assertThat(channelOpt.isPresent(), is(true));
        assertThat(channelOpt.get(), sameInstance(channel));
    }

    public void testChannelAttributeIsEmpty() {
        final Optional<ServerChannel> channelOpt = netty4HttpChannel.get("channel", ServerChannel.class);
        assertThat(channelOpt.isEmpty(), is(true));
    }

    public void testResponseAfterPipelineTeardownIsReleased() {
        assertRejectedResponseIsReleased(true, false);
    }

    public void testCloseDuringWriteReleasesResponseBeforePipelining() {
        assertRejectedResponseIsReleased(false, false);
    }

    public void testCancelledWriteReleasesResponseBeforePipelining() {
        assertRejectedResponseIsReleased(false, true);
    }

    private void assertRejectedResponseIsReleased(boolean closeBeforeSend, boolean cancelWrite) {
        final Netty4HttpPipeliningHandler pipelining = new Netty4HttpPipeliningHandler(logger, 3);
        final EmbeddedChannel embedded = new EmbeddedChannel(pipelining, new ChannelOutboundHandlerAdapter() {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                if (cancelWrite) {
                    promise.cancel(false);
                } else {
                    ctx.channel().close();
                    ((EmbeddedChannel) ctx.channel()).runPendingTasks();
                    assertNull(ctx.pipeline().context(pipelining));
                }
                ctx.write(msg, promise);
            }
        });
        final Netty4HttpResponse response = newResponse();
        final AtomicReference<Exception> failure = new AtomicReference<>();
        final AtomicInteger completions = new AtomicInteger();
        final AtomicInteger referencesAtFailure = new AtomicInteger(-1);
        try {
            final Netty4HttpChannel httpChannel = new Netty4HttpChannel(embedded);
            if (closeBeforeSend) {
                embedded.close();
                embedded.runPendingTasks();
                assertNull(embedded.pipeline().context(pipelining));
            }
            httpChannel.sendResponse(new HttpPipelinedResponse(0, response), ActionListener.wrap(r -> completions.incrementAndGet(), e -> {
                failure.set(e);
                referencesAtFailure.set(response.refCnt());
                completions.incrementAndGet();
            }));
            embedded.runPendingTasks();
            assertEquals(1, completions.get());
            if (cancelWrite) {
                assertTrue(failure.get() instanceof CancellationException);
            } else {
                assertTrue(failure.get() instanceof ClosedChannelException);
                assertEquals("Release the buffer before the failure listener frees its backing storage", 0, referencesAtFailure.get());
            }
            assertNull(embedded.readOutbound());
            assertEquals(0, response.refCnt());
        } finally {
            embedded.finishAndReleaseAll();
            if (response.refCnt() > 0) {
                response.release();
            }
        }
    }

    public void testSuccessfulWriteTransfersResponseToNetty() {
        final EmbeddedChannel embedded = new EmbeddedChannel(new Netty4HttpPipeliningHandler(logger, 3));
        final Netty4HttpResponse response = newResponse();
        final AtomicInteger completions = new AtomicInteger();
        final AtomicReference<Exception> failure = new AtomicReference<>();
        try {
            new Netty4HttpChannel(embedded).sendResponse(
                new HttpPipelinedResponse(0, response),
                ActionListener.wrap(r -> completions.incrementAndGet(), failure::set)
            );
            assertNull(failure.get());
            assertEquals(1, completions.get());
            assertSame(response, embedded.readOutbound());
            assertEquals("The outbound buffer owns the response until it is consumed", 1, response.refCnt());
            assertTrue(response.release());
            assertEquals(0, response.refCnt());
            assertNull(embedded.readOutbound());
        } finally {
            embedded.finishAndReleaseAll();
            if (response.refCnt() > 0) {
                response.release();
            }
        }
    }

    private static Netty4HttpResponse newResponse() {
        return new Netty4HttpResponse(EmptyHttpHeaders.INSTANCE, HttpVersion.HTTP_1_1, RestStatus.OK, new BytesArray("response"));
    }
}

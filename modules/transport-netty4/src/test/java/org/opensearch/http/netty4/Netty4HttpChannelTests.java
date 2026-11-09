/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.Netty4NioSocketChannel;
import org.junit.Before;

import java.util.Optional;

import io.netty.channel.Channel;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.channel.ServerChannel;

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
}

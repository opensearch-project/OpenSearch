/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.common.concurrent.CompletableContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpResponse;
import org.opensearch.transport.reactor.netty4.Netty4Utils;

import java.net.InetSocketAddress;
import java.util.Optional;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;

import static org.opensearch.http.reactor.netty4.ReactorNetty4BaseHttpChannel.CHANNEL_PROPERTY;

/**
 * A Reactor Netty 4 implementation of an {@link HttpChannel}.
 */
public class ReactorNetty4HttpChannel implements HttpChannel {
    private final Channel channel;
    private final ChannelPipeline inboundPipeline;
    private final CompletableContext<Void> closeContext = new CompletableContext<>();

    ReactorNetty4HttpChannel(Channel channel) {
        this.channel = channel;
        this.inboundPipeline = channel.pipeline();
        Netty4Utils.addListener(this.channel.closeFuture(), closeContext);
    }

    @Override
    public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
        channel.writeAndFlush(response, Netty4Utils.addPromise(listener, channel));
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) channel.remoteAddress();
    }

    @Override
    public void close() {
        channel.close();
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        closeContext.addListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    public ChannelPipeline inboundPipeline() {
        return inboundPipeline;
    }

    public Channel getNettyChannel() {
        return channel;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Optional<T> get(String name, Class<T> clazz) {
        if (CHANNEL_PROPERTY.equalsIgnoreCase(name) && clazz.isAssignableFrom(Channel.class)) {
            return (Optional<T>) Optional.of(getNettyChannel());
        }

        Object handler = getNettyChannel().pipeline().get(name);

        if (handler == null && inboundPipeline() != null) {
            handler = inboundPipeline().get(name);
        }

        if (handler != null && clazz.isInstance(handler) == true) {
            return Optional.of((T) handler);
        }

        return Optional.empty();
    }
}

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

import org.opensearch.common.Nullable;
import org.opensearch.common.concurrent.CompletableContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpResponse;
import org.opensearch.transport.netty4.Netty4TcpChannel;

import javax.net.ssl.SSLEngine;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Optional;
import java.util.function.Function;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.util.AttributeKey;

public class Netty4HttpChannel implements HttpChannel {
    private static final InetSocketAddress NO_SOCKET_ADDRESS = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);

    private static final String CHANNEL_PROPERTY = "channel";
    private static final String SSL_ENGINE_PROPERTY = "ssl_engine";

    private final Channel channel;
    private final CompletableContext<Void> closeContext = new CompletableContext<>();
    private final ChannelPipeline inboundPipeline;
    private final AttributeKey<SSLEngine> sslEngineKey;

    Netty4HttpChannel(Channel channel) {
        this(channel, null, null);
    }

    Netty4HttpChannel(Channel channel, AttributeKey<SSLEngine> sslEngineKey) {
        this(channel, null, sslEngineKey);
    }

    Netty4HttpChannel(Channel channel, ChannelPipeline inboundPipeline) {
        this(channel, inboundPipeline, null);
    }

    Netty4HttpChannel(Channel channel, ChannelPipeline inboundPipeline, AttributeKey<SSLEngine> sslEngineKey) {
        this.channel = channel;
        this.inboundPipeline = inboundPipeline;
        this.sslEngineKey = sslEngineKey;
        Netty4TcpChannel.addListener(this.channel.closeFuture(), closeContext);
    }

    @Override
    public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
        channel.writeAndFlush(response, Netty4TcpChannel.addPromise(listener, channel));
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        if (channel.localAddress() instanceof InetSocketAddress isa) {
            return isa;
        } else {
            return getAddressFromParent(channel, Channel::localAddress);
        }
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        if (channel.remoteAddress() instanceof InetSocketAddress isa) {
            return isa;
        } else {
            final InetSocketAddress address = getAddressFromParent(channel, Channel::remoteAddress);
            if (address == null && channel.remoteAddress() != null) {
                // In case of QUIC / HTTP3, the datagram channel (parent) may not have address
                // populated, but QuicChannelXxx does, returning the placeholder with 0 port here.
                return NO_SOCKET_ADDRESS;
            }
            return address;
        }
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        closeContext.addListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public void close() {
        channel.close();
    }

    public @Nullable ChannelPipeline inboundPipeline() {
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

        if (sslEngineKey != null) {
            if (SSL_ENGINE_PROPERTY.equalsIgnoreCase(name) && clazz.isAssignableFrom(SSLEngine.class)) {
                SSLEngine engine = channel.attr(sslEngineKey).get();
                if (engine == null && channel.parent() != null) {
                    engine = channel.parent().attr(sslEngineKey).get();
                }
                if (engine != null) {
                    return (Optional<T>) Optional.of(engine);
                }
            }
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

    @Override
    public String toString() {
        return "Netty4HttpChannel{" + "localAddress=" + getLocalAddress() + ", remoteAddress=" + getRemoteAddress() + '}';
    }

    /**
     * Attempts to extract the {@link InetSocketAddress} from parent channel, since
     * some channels, like QuicXxxChannel, do not expose {@link InetSocketAddress} but
     * {@link SocketAddress} only
     */
    private InetSocketAddress getAddressFromParent(Channel channel, Function<Channel, SocketAddress> socketAddressSupplier) {
        final Channel parent = channel.parent();
        if (parent != null) {
            if (socketAddressSupplier.apply(parent) instanceof InetSocketAddress isa) {
                return isa;
            } else {
                return getAddressFromParent(parent, socketAddressSupplier);
            }
        } else {
            return null; /* Not connected */
        }
    }
}

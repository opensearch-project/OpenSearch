/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import javax.net.ssl.SSLEngine;

import java.util.Optional;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.handler.ssl.SslHandler;
import reactor.netty.NettyPipeline;
import reactor.netty.http.server.HttpServerRequest;

final class ReactorNetty4BaseHttpChannel {
    private static final String CHANNEL_PROPERTY = "channel";
    private static final String SSL_HANDLER_PROPERTY = "ssl_http";
    private static final String SSL_ENGINE_PROPERTY = "ssl_engine";

    private ReactorNetty4BaseHttpChannel() {}

    @SuppressWarnings("unchecked")
    static <T> Optional<T> get(HttpServerRequest request, String name, Class<T> clazz) {
        if (CHANNEL_PROPERTY.equalsIgnoreCase(name) == true && clazz.isAssignableFrom(Channel.class) == true) {
            final Channel[] channels = new Channel[1];
            request.withConnection(connection -> { channels[0] = connection.channel(); });
            return Optional.of((T) channels[0]);
        } else if (SSL_HANDLER_PROPERTY.equalsIgnoreCase(name) == true || SSL_ENGINE_PROPERTY.equalsIgnoreCase(name) == true) {
            final ChannelHandler[] channels = new ChannelHandler[1];
            request.withConnection(connection -> {
                final Channel channel = connection.channel();
                if (channel.parent() != null) {
                    channels[0] = channel.parent().pipeline().get(NettyPipeline.SslHandler);
                } else {
                    channels[0] = channel.pipeline().get(NettyPipeline.SslHandler);
                }
            });
            if (channels[0] != null) {
                if (SSL_HANDLER_PROPERTY.equalsIgnoreCase(name) == true && clazz.isInstance(channels[0]) == true) {
                    return Optional.of((T) channels[0]);
                } else if (SSL_ENGINE_PROPERTY.equalsIgnoreCase(name) == true
                    && clazz.isAssignableFrom(SSLEngine.class)
                    && channels[0] instanceof SslHandler h) {
                        return Optional.of((T) h.engine());
                    }
            }
        } else {
            final ChannelHandler[] channels = new ChannelHandler[1];
            request.withConnection(connection -> { channels[0] = connection.channel().pipeline().get(name); });
            if (channels[0] != null && clazz.isInstance(channels[0]) == true) {
                return Optional.of((T) channels[0]);
            }
        }

        return Optional.empty();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.List;

import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.internal.SocketUtils;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

public class Netty4NioServerSocketChannel extends NioServerSocketChannel {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(Netty4NioServerSocketChannel.class);

    public Netty4NioServerSocketChannel() {
        super();
    }

    public Netty4NioServerSocketChannel(SelectorProvider provider) {
        super(provider);
    }

    public Netty4NioServerSocketChannel(SelectorProvider provider, InternetProtocolFamily family) {
        super(provider, family);
    }

    public Netty4NioServerSocketChannel(ServerSocketChannel channel) {
        super(channel);
    }

    @Override
    protected int doReadMessages(List<Object> buf) throws Exception {
        SocketChannel ch = SocketUtils.accept(javaChannel());

        try {
            if (ch != null) {
                buf.add(new Netty4NioSocketChannel(this, ch));
                return 1;
            }
        } catch (Throwable t) {
            logger.warn("Failed to create a new channel from an accepted socket.", t);

            try {
                ch.close();
            } catch (Throwable t2) {
                logger.warn("Failed to close a socket.", t2);
            }
        }

        return 0;
    }
}

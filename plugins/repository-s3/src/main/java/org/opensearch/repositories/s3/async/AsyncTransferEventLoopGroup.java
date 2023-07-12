/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.repositories.s3.SocketAccess;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

/**
 * AsyncTransferEventLoopGroup is an encapsulation for netty {@link EventLoopGroup}
 */
public class AsyncTransferEventLoopGroup implements Closeable {
    private static final String THREAD_PREFIX = "s3-async-transfer-worker";
    private final Logger logger = LogManager.getLogger(AsyncTransferEventLoopGroup.class);

    private final EventLoopGroup eventLoopGroup;

    /**
     * Construct a new AsyncTransferEventLoopGroup
     *
     * @param eventLoopThreads The number of event loop threads for this event loop group
     */
    public AsyncTransferEventLoopGroup(int eventLoopThreads) {
        // Epoll event loop incurs less GC and provides better performance than Nio loop. Therefore,
        // using epoll wherever available is preferred.
        this.eventLoopGroup = SocketAccess.doPrivileged(
            () -> Epoll.isAvailable()
                ? new EpollEventLoopGroup(eventLoopThreads, OpenSearchExecutors.daemonThreadFactory(THREAD_PREFIX))
                : new NioEventLoopGroup(eventLoopThreads, OpenSearchExecutors.daemonThreadFactory(THREAD_PREFIX))
        );
    }

    public EventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
    }

    @Override
    public void close() {
        Future<?> shutdownFuture = eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        shutdownFuture.awaitUninterruptibly();
        if (!shutdownFuture.isSuccess()) {
            logger.warn(new ParameterizedMessage("Error closing {} netty event loop group", THREAD_PREFIX), shutdownFuture.cause());
        }
    }

}

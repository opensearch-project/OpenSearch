package org.opensearch.repositories.s3.async;


import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.repositories.s3.SocketAccess;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;


public class TransferNIOGroup implements Closeable {
    private static final int MAX_EVENT_LOOP_THREADS = 8;
    private static final String THREAD_PREFIX = "aws-async-transfer-nio";
    private final Logger logger = LogManager.getLogger(TransferNIOGroup.class);

    private final EventLoopGroup eventLoopGroup;

    public TransferNIOGroup() {
        // Epoll event loop incurs less GC and provides better performance than Nio loop. Therefore,
        // using epoll wherever available is preferred.
        this.eventLoopGroup = SocketAccess.doPrivileged(()->Epoll.isAvailable() ?
            new EpollEventLoopGroup(MAX_EVENT_LOOP_THREADS,
                new OpenSearchThreadFactory(THREAD_PREFIX)):
                new NioEventLoopGroup(MAX_EVENT_LOOP_THREADS, new OpenSearchThreadFactory(THREAD_PREFIX)));
    }

    public EventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
    }

    @Override
    public void close() {
        Future<?> shutdownFuture = eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        shutdownFuture.awaitUninterruptibly();
        if (!shutdownFuture.isSuccess()) {
            logger.warn("Error closing {} netty event loop group", THREAD_PREFIX, shutdownFuture.cause());
        }
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.transport.reactor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.reactor.netty4.ReactorNetty4HttpServerTransport;
import org.opensearch.transport.TcpTransport;
import org.opensearch.transport.reactor.netty4.ReactorNetty4Transport;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;

import static org.opensearch.common.util.concurrent.OpenSearchExecutors.daemonThreadFactory;

/**
 * Creates and returns {@link io.netty.channel.EventLoopGroup} instances. It will return a shared group for
 * both {@link #getHttpGroup()} and {@link #getTransportGroup()} if
 * {@link org.opensearch.http.reactor.netty4.ReactorNetty4HttpServerTransport#SETTING_HTTP_WORKER_COUNT} is configured to be 0.
 * If that setting is not 0, then it will return a different group in the {@link #getHttpGroup()} call.
 */
public final class SharedGroupFactory {

    private static final Logger logger = LogManager.getLogger(SharedGroupFactory.class);

    private final Settings settings;
    private final int workerCount;
    private final int httpWorkerCount;

    private RefCountedGroup genericGroup;
    private SharedGroup dedicatedHttpGroup;

    /**
     * Creates new shared group factory instance from settings
     * @param settings settings
     */
    public SharedGroupFactory(Settings settings) {
        this.settings = settings;
        this.workerCount = ReactorNetty4Transport.SETTING_WORKER_COUNT.get(settings);
        this.httpWorkerCount = ReactorNetty4HttpServerTransport.SETTING_HTTP_WORKER_COUNT.get(settings);
    }

    Settings getSettings() {
        return settings;
    }

    /**
     * Gets the number of configured transport workers
     * @return the number of configured transport workers
     */
    public int getTransportWorkerCount() {
        return workerCount;
    }

    /**
     * Gets transport shared group
     * @return transport shared group
     */
    public synchronized SharedGroup getTransportGroup() {
        return getGenericGroup();
    }

    /**
     * Gets HTTP transport shared group
     * @return HTTP transport shared group
     */
    public synchronized SharedGroup getHttpGroup() {
        if (httpWorkerCount == 0) {
            return getGenericGroup();
        } else {
            if (dedicatedHttpGroup == null) {
                NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(
                    httpWorkerCount,
                    daemonThreadFactory(settings, HttpServerTransport.HTTP_SERVER_WORKER_THREAD_NAME_PREFIX)
                );
                dedicatedHttpGroup = new SharedGroup(new RefCountedGroup(eventLoopGroup));
            }
            return dedicatedHttpGroup;
        }
    }

    private SharedGroup getGenericGroup() {
        if (genericGroup == null) {
            EventLoopGroup eventLoopGroup = new NioEventLoopGroup(
                workerCount,
                daemonThreadFactory(settings, TcpTransport.TRANSPORT_WORKER_THREAD_NAME_PREFIX)
            );
            this.genericGroup = new RefCountedGroup(eventLoopGroup);
        } else {
            genericGroup.incRef();
        }
        return new SharedGroup(genericGroup);
    }

    private static class RefCountedGroup extends AbstractRefCounted {

        public static final String NAME = "ref-counted-event-loop-group";
        private final EventLoopGroup eventLoopGroup;

        private RefCountedGroup(EventLoopGroup eventLoopGroup) {
            super(NAME);
            this.eventLoopGroup = eventLoopGroup;
        }

        @Override
        protected void closeInternal() {
            Future<?> shutdownFuture = eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
            shutdownFuture.awaitUninterruptibly();
            if (shutdownFuture.isSuccess() == false) {
                logger.warn("Error closing netty event loop group", shutdownFuture.cause());
            }
        }
    }

    /**
     * Wraps the {@link RefCountedGroup}. Calls {@link RefCountedGroup#decRef()} on close. After close,
     * this wrapped instance can no longer be used.
     */
    public static class SharedGroup {

        private final RefCountedGroup refCountedGroup;

        private final AtomicBoolean isOpen = new AtomicBoolean(true);

        private SharedGroup(RefCountedGroup refCountedGroup) {
            this.refCountedGroup = refCountedGroup;
        }

        /**
         * Gets Netty's {@link EventLoopGroup} instance
         * @return Netty's {@link EventLoopGroup} instance
         */
        public EventLoopGroup getLowLevelGroup() {
            return refCountedGroup.eventLoopGroup;
        }

        /**
         * Decreases the reference to underlying  {@link EventLoopGroup} instance
         */
        public void shutdown() {
            if (isOpen.compareAndSet(true, false)) {
                refCountedGroup.decRef();
            }
        }
    }
}

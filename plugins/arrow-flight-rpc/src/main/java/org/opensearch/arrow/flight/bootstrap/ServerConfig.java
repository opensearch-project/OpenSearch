/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.bootstrap;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.threadpool.ScalingExecutorBuilder;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.NettyRuntime;

/**
 * Configuration class for OpenSearch Flight server settings.
 * This class manages server-side configurations including port settings, Arrow memory settings,
 * thread pool configurations, and SSL/TLS settings.
 * @opensearch.internal
 */
public class ServerConfig {
    /**
     * Creates a new instance of the server configuration with default settings.
     */
    public ServerConfig() {}

    static final Setting<String> ARROW_ALLOCATION_MANAGER_TYPE = Setting.simpleString(
        "arrow.allocation.manager.type",
        "Netty",
        Setting.Property.NodeScope
    );

    static final Setting<Boolean> ARROW_ENABLE_NULL_CHECK_FOR_GET = Setting.boolSetting(
        "arrow.enable_null_check_for_get",
        false,
        Setting.Property.NodeScope
    );

    static final Setting<Boolean> ARROW_ENABLE_DEBUG_ALLOCATOR = Setting.boolSetting(
        "arrow.memory.debug.allocator",
        false,
        Setting.Property.NodeScope
    );

    static final Setting<Boolean> ARROW_ENABLE_UNSAFE_MEMORY_ACCESS = Setting.boolSetting(
        "arrow.enable_unsafe_memory_access",
        true,
        Setting.Property.NodeScope
    );

    static final Setting<Integer> FLIGHT_THREAD_POOL_MIN_SIZE = Setting.intSetting(
        "thread_pool.flight-server.min",
        0,
        0,
        Setting.Property.NodeScope
    );

    static final Setting<Integer> FLIGHT_THREAD_POOL_MAX_SIZE = Setting.intSetting(
        "thread_pool.flight-server.max",
        100000, // TODO depends on max concurrent streams per node, decide after benchmark. To be controlled by admission control layer.
        1,
        Setting.Property.NodeScope
    );

    static final Setting<TimeValue> FLIGHT_THREAD_POOL_KEEP_ALIVE = Setting.timeSetting(
        "thread_pool.flight-server.keep_alive",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );

    static final Setting<Integer> FLIGHT_EVENT_LOOP_THREADS = Setting.intSetting(
        "flight.event_loop.threads",
        Math.max(1, NettyRuntime.availableProcessors() * 2),
        1,
        Setting.Property.NodeScope
    );

    static final Setting<Boolean> ARROW_SSL_ENABLE = Setting.boolSetting(
        "flight.ssl.enable",
        false, // TODO: get default from security enabled
        Setting.Property.NodeScope
    );

    /**
     * The thread pool name for the Flight producer handling
     */
    public static final String FLIGHT_SERVER_THREAD_POOL_NAME = "flight-server";
    /**
     * The thread pool name for the Flight grpc executor.
     */
    public static final String GRPC_EXECUTOR_THREAD_POOL_NAME = "flight-grpc";

    /**
     * The thread pool name for the Flight client.
     */
    public static final String FLIGHT_CLIENT_THREAD_POOL_NAME = "flight-client";

    private static final String host = "localhost";
    private static boolean enableSsl;
    private static int threadPoolMin;
    private static int threadPoolMax;
    private static TimeValue keepAlive;
    private static int eventLoopThreads;

    /**
     * Initializes the server configuration with the provided settings.
     * Sets system properties for Arrow memory management and configures thread pool settings.
     *
     * @param settings The OpenSearch settings to initialize the server with
     */
    @SuppressForbidden(reason = "required for arrow allocator")
    @SuppressWarnings("removal")
    public static void init(Settings settings) {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            System.setProperty("arrow.allocation.manager.type", ARROW_ALLOCATION_MANAGER_TYPE.get(settings));
            System.setProperty("arrow.enable_null_check_for_get", Boolean.toString(ARROW_ENABLE_NULL_CHECK_FOR_GET.get(settings)));
            System.setProperty("arrow.enable_unsafe_memory_access", Boolean.toString(ARROW_ENABLE_UNSAFE_MEMORY_ACCESS.get(settings)));
            System.setProperty("arrow.memory.debug.allocator", Boolean.toString(ARROW_ENABLE_DEBUG_ALLOCATOR.get(settings)));
            Netty4Configs.init(settings);
            return null;
        });
        enableSsl = ARROW_SSL_ENABLE.get(settings);
        threadPoolMin = FLIGHT_THREAD_POOL_MIN_SIZE.get(settings);
        threadPoolMax = FLIGHT_THREAD_POOL_MAX_SIZE.get(settings);
        keepAlive = FLIGHT_THREAD_POOL_KEEP_ALIVE.get(settings);
        eventLoopThreads = FLIGHT_EVENT_LOOP_THREADS.get(settings);
    }

    /**
     * Checks if SSL/TLS is enabled for the Flight server.
     *
     * @return true if SSL is enabled, false otherwise
     */
    public static boolean isSslEnabled() {
        return enableSsl;
    }

    /**
     * Gets the thread pool executor builder configured for the Flight server.
     *
     * @return The configured ScalingExecutorBuilder instance
     */
    public static ScalingExecutorBuilder getServerExecutorBuilder() {
        return new ScalingExecutorBuilder(FLIGHT_SERVER_THREAD_POOL_NAME, threadPoolMin, threadPoolMax, keepAlive);
    }

    /**
     * Gets the thread pool executor builder configured for the Flight server grpc executor.
     *
     * @return The configured ScalingExecutorBuilder instance
     */
    public static ScalingExecutorBuilder getGrpcExecutorBuilder() {
        return new ScalingExecutorBuilder(GRPC_EXECUTOR_THREAD_POOL_NAME, threadPoolMin, threadPoolMax, keepAlive);
    }

    /**
     * Gets the thread pool executor builder configured for the Flight server.
     *
     * @return The configured ScalingExecutorBuilder instance
     */
    public static ScalingExecutorBuilder getClientExecutorBuilder() {
        return new ScalingExecutorBuilder(FLIGHT_CLIENT_THREAD_POOL_NAME, threadPoolMin, threadPoolMax, keepAlive);
    }

    /**
     * Gets the configured number of event loop threads.
     *
     * @return The number of event loop threads
     */
    public static int getEventLoopThreads() {
        return eventLoopThreads;
    }

    /**
     * Returns a list of all settings managed by this configuration class.
     *
     * @return List of Setting instances
     */
    public static List<Setting<?>> getSettings() {
        return new ArrayList<>(
            Arrays.asList(
                ARROW_ALLOCATION_MANAGER_TYPE,
                ARROW_ENABLE_NULL_CHECK_FOR_GET,
                ARROW_ENABLE_DEBUG_ALLOCATOR,
                ARROW_ENABLE_UNSAFE_MEMORY_ACCESS,
                ARROW_SSL_ENABLE,
                FLIGHT_EVENT_LOOP_THREADS,
                FLIGHT_THREAD_POOL_MIN_SIZE
            )
        );
    }

    static EventLoopGroup createELG(String name, int eventLoopThreads) {

        return Epoll.isAvailable()
            ? new EpollEventLoopGroup(eventLoopThreads, OpenSearchExecutors.daemonThreadFactory(name))
            : new NioEventLoopGroup(eventLoopThreads, OpenSearchExecutors.daemonThreadFactory(name));
    }

    /**
     * Returns the appropriate server channel type based on platform availability.
     *
     * @return EpollServerSocketChannel if Epoll is available, otherwise NioServerSocketChannel
     */
    public static Class<? extends Channel> serverChannelType() {
        return Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
    }

    /**
     * Returns the appropriate client channel type based on platform availability.
     *
     * @return EpollSocketChannel if Epoll is available, otherwise NioSocketChannel
     */
    public static Class<? extends Channel> clientChannelType() {
        return Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class;
    }

    private static class Netty4Configs {

        @SuppressForbidden(reason = "required for netty allocator configuration")
        public static void init(Settings settings) {
            int numArenas = Integer.parseInt(System.getProperty("io.netty.allocator.numDirectArenas"));
            if (numArenas <= 0) {
                throw new IllegalStateException("io.netty.allocator.numDirectArenas must be > 0");
            }
            checkSystemProperty("io.netty.noUnsafe", "false");
            checkSystemProperty("io.netty.tryUnsafe", "true");
            checkSystemProperty("io.netty.tryReflectionSetAccessible", "true");
        }

        private static void checkSystemProperty(String propertyName, String expectedValue) {
            String actualValue = System.getProperty(propertyName);
            if (!expectedValue.equals(actualValue)) {
                throw new IllegalStateException(
                    "Required system property ["
                        + propertyName
                        + "] is incorrect; expected: ["
                        + expectedValue
                        + "] actual: ["
                        + actualValue
                        + "]."
                );
            }
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.arrow.flight;

import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import org.apache.arrow.memory.BufferAllocator;

import java.util.concurrent.ExecutorService;

/**
 * Builder for FlightServer in OpenSearch. Overrides {@link OSFlightServer.Builder} to set SslContext
 */
public class OSFlightServerBuilder extends OSFlightServer.Builder {

    private SslContext sslContext;

    OSFlightServerBuilder(BufferAllocator allocator, Location location, FlightProducer producer, SslContext sslContext) {
        super(allocator, location, producer);
        this.sslContext = sslContext;
    }

    /**
     * Create a new builder for a FlightServer.
     * @param allocator BufferAllocator to use for this server.
     * @param location  Location to bind to.
     * @param producer  FlightProducer to use for this server.
     * @param sslContext SslContext to use for this server.
     * @param channelType Channel type to use for this server.
     * @param bossELG EventLoopGroup to use for this server.
     * @param workerELG EventLoopGroup to use for this server.
     * @param grpcExecutor ExecutorService to use for this server.
     * @return OSFlightServerBuilder
     */
    public static OSFlightServerBuilder builder(
        BufferAllocator allocator,
        Location location,
        FlightProducer producer,
        SslContext sslContext,
        Class<? extends Channel> channelType,
        EventLoopGroup bossELG,
        EventLoopGroup workerELG,
        ExecutorService grpcExecutor
    ) {
        OSFlightServerBuilder builder = new OSFlightServerBuilder(allocator, location, producer, sslContext);
        builder.transportHint("netty.channelType", channelType);
        builder.transportHint("netty.bossEventLoopGroup", bossELG);
        builder.transportHint("netty.workerEventLoopGroup", workerELG);
        builder.executor(grpcExecutor);
        return builder;
    }

    @Override
    public void configureBuilder(NettyServerBuilder nettyServerBuilder) {
        if (sslContext != null) {
            nettyServerBuilder.sslContext(sslContext);
        }
    }
}

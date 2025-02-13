/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.arrow.flight;

import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import org.apache.arrow.memory.BufferAllocator;

import java.util.concurrent.ExecutorService;

/**
 * Builder for FlightClient in OpenSearch. Overrides {@link OSFlightClient.Builder} to set SslContext, workerELG,
 * executorService and channelType
 */
public class OSFlightClientBuilder extends OSFlightClient.Builder {
    private EventLoopGroup workerELG;
    private ExecutorService executorService;
    private Class<? extends io.netty.channel.Channel> channelType;
    private SslContext sslContext;

    public OSFlightClientBuilder(BufferAllocator allocator,
                                 Location location,
                                 Class<? extends io.netty.channel.Channel> channelType,
                                 ExecutorService executorService,
                                 EventLoopGroup workerELG,
                                 SslContext sslContext) {
        super(allocator, location);
        this.channelType = channelType;
        this.executorService = executorService;
        this.workerELG = workerELG;
        if (sslContext != null) {
            this.sslContext = sslContext;
        }
    }

    @Override
    public void configureBuilder(NettyChannelBuilder builder) {
        if (workerELG != null) {
            builder.eventLoopGroup(workerELG);
        }
        if (executorService != null) {
            builder.executor(executorService);
        }
        if (channelType != null) {
            builder.channelType(channelType);
        }
        if (sslContext != null) {
            builder.sslContext(sslContext);
        }
    }
}

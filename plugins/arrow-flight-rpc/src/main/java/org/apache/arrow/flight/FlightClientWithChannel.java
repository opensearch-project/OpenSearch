/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.arrow.flight;

import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;

import java.util.List;

/**
 * Extends FlightClient to expose the underlying ManagedChannel.
 * This allows DemandDrivenFlightStream to create ClientCalls directly
 * without reflection, enabling disableAutoInboundFlowControl().
 */
public class FlightClientWithChannel extends FlightClient {

    private final ManagedChannel channel;
    private final BufferAllocator allocator;

    FlightClientWithChannel(BufferAllocator allocator, ManagedChannel channel, List<FlightClientMiddleware.Factory> middleware) {
        super(allocator, channel, middleware);
        this.channel = channel;
        this.allocator = allocator;
    }

    /** Returns the underlying gRPC channel for demand-driven stream creation. */
    public ManagedChannel getChannel() {
        return channel;
    }

    /** Returns the allocator used for Arrow deserialization. */
    public BufferAllocator getAllocator() {
        return allocator;
    }
}

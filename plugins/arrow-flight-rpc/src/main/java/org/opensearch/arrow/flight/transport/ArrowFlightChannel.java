/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.transport.BaseTcpTransportChannel;
import org.opensearch.transport.TaskTransportChannel;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TransportChannel;

/**
 * Provides access to the Arrow {@link BufferAllocator} for request handlers
 * that produce native Arrow responses.
 *
 * <p>Use {@link #from(TransportChannel)} to obtain an instance from any
 * {@code TransportChannel}, regardless of wrapper layers.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface ArrowFlightChannel {

    /**
     * Returns the Arrow allocator for this channel.
     */
    BufferAllocator getAllocator();

    /**
     * Unwraps the given {@link TransportChannel} to find the {@link ArrowFlightChannel}.
     * Walks through {@link TaskTransportChannel} and {@link BaseTcpTransportChannel}
     * wrapper layers to find the underlying channel.
     *
     * @param channel the transport channel (may be wrapped)
     * @return the ArrowFlightChannel
     * @throws IllegalArgumentException if the channel is not backed by an ArrowFlightChannel
     */
    static ArrowFlightChannel from(TransportChannel channel) {
        TransportChannel current = channel;
        while (current != null) {
            if (current instanceof ArrowFlightChannel afc) {
                return afc;
            }
            if (current instanceof TaskTransportChannel ttc) {
                current = ttc.getChannel();
            } else if (current instanceof BaseTcpTransportChannel btc) {
                TcpChannel tcpChannel = btc.getChannel();
                if (tcpChannel instanceof ArrowFlightChannel afc) {
                    return afc;
                }
                break;
            } else {
                break;
            }
        }
        throw new IllegalArgumentException("Channel is not backed by an ArrowFlightChannel: " + channel.getClass().getName());
    }
}

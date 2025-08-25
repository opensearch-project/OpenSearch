/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.Version;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TcpChannel;

import java.util.Set;

/**
 * Represents a batch processing task for Flight streaming responses.
 * @opensearch.internal
 */
class BatchTask implements AutoCloseable {
    final Version nodeVersion;
    final Set<String> features;
    final TcpChannel channel;
    final FlightTransportChannel transportChannel;
    final long requestId;
    final String action;
    final TransportResponse response;
    final boolean compress;
    final boolean isHandshake;
    final boolean isComplete;
    final boolean isError;
    final Exception error;
    final ThreadContext.StoredContext storedContext;

    BatchTask(
        Version nodeVersion,
        Set<String> features,
        TcpChannel channel,
        FlightTransportChannel transportChannel,
        long requestId,
        String action,
        TransportResponse response,
        boolean compress,
        boolean isHandshake,
        boolean isComplete,
        ThreadContext.StoredContext storedContext
    ) {
        this(
            nodeVersion,
            features,
            channel,
            transportChannel,
            requestId,
            action,
            response,
            compress,
            isHandshake,
            isComplete,
            false,
            null,
            storedContext
        );
    }

    BatchTask(
        Version nodeVersion,
        Set<String> features,
        TcpChannel channel,
        FlightTransportChannel transportChannel,
        long requestId,
        String action,
        boolean compress,
        boolean isHandshake,
        Exception error,
        ThreadContext.StoredContext storedContext
    ) {
        this(
            nodeVersion,
            features,
            channel,
            transportChannel,
            requestId,
            action,
            null,
            compress,
            isHandshake,
            false,
            true,
            error,
            storedContext
        );
    }

    private BatchTask(
        Version nodeVersion,
        Set<String> features,
        TcpChannel channel,
        FlightTransportChannel transportChannel,
        long requestId,
        String action,
        TransportResponse response,
        boolean compress,
        boolean isHandshake,
        boolean isComplete,
        boolean isError,
        Exception error,
        ThreadContext.StoredContext storedContext
    ) {
        this.nodeVersion = nodeVersion;
        this.features = features;
        this.channel = channel;
        this.transportChannel = transportChannel;
        this.requestId = requestId;
        this.action = action;
        this.response = response;
        this.compress = compress;
        this.isHandshake = isHandshake;
        this.isComplete = isComplete;
        this.isError = isError;
        this.error = error;
        this.storedContext = storedContext;
    }

    @Override
    public void close() {
        if (storedContext != null) {
            storedContext.close();
        }
        if ((isComplete || isError) && transportChannel != null) {
            transportChannel.releaseChannel(isError);
        }
    }
}

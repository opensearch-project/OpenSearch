/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.arrow.flight.stats.FlightStatsCollector;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TcpTransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.stream.StreamCancellationException;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A TCP transport channel for Arrow Flight, supporting only streaming responses.
 * It is released in case any exception occurs in sendResponseBatch when sendResponse(Exception)
 * is called or when completeStream() is called.
 * The underlying TcpChannel is closed when release is called.
 * @opensearch.internal
 */
class FlightTransportChannel extends TcpTransportChannel {
    private static final Logger logger = LogManager.getLogger(FlightTransportChannel.class);

    private final AtomicBoolean streamOpen = new AtomicBoolean(true);
    private final FlightStatsCollector statsCollector;

    public FlightTransportChannel(
        FlightOutboundHandler outboundHandler,
        TcpChannel channel,
        String action,
        long requestId,
        Version version,
        Set<String> features,
        boolean compressResponse,
        boolean isHandshake,
        Releasable breakerRelease,
        FlightStatsCollector statsCollector
    ) {
        super(outboundHandler, channel, action, requestId, version, features, compressResponse, isHandshake, breakerRelease);
        this.statsCollector = statsCollector;
    }

    @Override
    public void sendResponse(TransportResponse response) {
        throw new UnsupportedOperationException("Use sendResponseBatch instead");
    }

    @Override
    public void sendResponse(Exception exception) throws IOException {
        super.sendResponse(exception);
    }

    @Override
    public void sendResponseBatch(TransportResponse response) {
        if (!streamOpen.get()) {
            throw new TransportException("Stream is closed for requestId [" + requestId + "]");
        }
        if (response instanceof QuerySearchResult && ((QuerySearchResult) response).getShardSearchRequest() != null) {
            ((QuerySearchResult) response).getShardSearchRequest().setOutboundNetworkTime(System.currentTimeMillis());
        }
        try {
            ((FlightOutboundHandler) outboundHandler).sendResponseBatch(
                version,
                features,
                getChannel(),
                requestId,
                action,
                response,
                compressResponse,
                isHandshake
            );
        } catch (StreamCancellationException e) {
            release(true);
            throw e;
        } catch (Exception e) {
            release(true);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void completeStream() {
        if (streamOpen.compareAndSet(true, false)) {
            try {
                ((FlightOutboundHandler) outboundHandler).completeStream(version, features, getChannel(), requestId, action);
                release(false);
            } catch (Exception e) {
                release(true);
                throw e;
            }
        } else {
            release(true);
            logger.warn("CompleteStream called on already closed stream with action[{}] and requestId[{}]", action, requestId);
            throw new TransportException("FlightTransportChannel stream already closed.");
        }
    }

    @Override
    protected void release(boolean isExceptionResponse) {
        getChannel().close();
        super.release(isExceptionResponse);
    }
}

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
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TcpTransportChannel;
import org.opensearch.transport.TransportException;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A TCP transport channel for Arrow Flight, supporting only streaming responses.
 *
 * @opensearch.internal
 */
public class FlightTransportChannel extends TcpTransportChannel {
    private static final Logger logger = LogManager.getLogger(FlightTransportChannel.class);

    private final AtomicBoolean streamOpen = new AtomicBoolean(true);

    public FlightTransportChannel(
        FlightOutboundHandler outboundHandler,
        TcpChannel channel,
        String action,
        long requestId,
        Version version,
        Set<String> features,
        boolean compressResponse,
        boolean isHandshake,
        Releasable breakerRelease
    ) {
        super(outboundHandler, channel, action, requestId, version, features, compressResponse, isHandshake, breakerRelease);
    }

    @Override
    public void sendResponse(Exception exception) throws IOException {
        try {
            outboundHandler.sendErrorResponse(version, features, getChannel(), requestId, action, exception);
            logger.debug("Sent error response for action [{}] with requestId [{}]", action, requestId);
        } finally {
            release(true);
        }
    }

    @Override
    public void sendResponseBatch(TransportResponse response) {
        if (!streamOpen.get()) {
            throw new TransportException("Stream is closed for requestId [" + requestId + "]");
        }
        if (response instanceof QuerySearchResult && ((QuerySearchResult) response).getShardSearchRequest() != null) {
            ((QuerySearchResult) response).getShardSearchRequest().setOutboundNetworkTime(System.currentTimeMillis());
        }
        ((FlightOutboundHandler) outboundHandler).sendResponseBatch(
            version,
            features,
            getChannel(),
            requestId,
            action,
            response,
            compressResponse,
            isHandshake,
            ActionListener.wrap(
                (resp) -> logger.debug("Response batch sent for action [{}] with requestId [{}]", action, requestId),
                e -> logger.error("Failed to send response batch for action [{}] with requestId [{}]", action, requestId, e)
            )
        );
    }

    @Override
    public void completeStream() {
        if (streamOpen.compareAndSet(true, false)) {
            ((FlightOutboundHandler) outboundHandler).completeStream(
                version,
                features,
                getChannel(),
                requestId,
                action,
                ActionListener.wrap(
                    (resp) -> {
                        logger.debug("Stream completed for action [{}] with requestId [{}]", action, requestId);
                        release(false);
                    },
                    e -> {
                        logger.error("Failed to complete stream for action [{}] with requestId [{}]", action, requestId, e);
                        release(true);
                    }
                )
            );
        } else {
            try {
                outboundHandler.sendErrorResponse(version,
                    features,
                    getChannel(),
                    requestId,
                    action,
                    new TransportException("FlightTransportChannel stream already closed.")
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                release(true);
            }
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;
import java.util.Set;

/**
 * Protocol based outbound data handler.
 * Different transport protocols can have different implementations of this class.
 * 
 * @opensearch.internal
 */
public abstract class ProtocolOutboundHandler {

    public abstract void sendRequest(
        final DiscoveryNode node,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final TransportRequest request,
        final TransportRequestOptions options,
        final Version channelVersion,
        final boolean compressRequest,
        final boolean isHandshake
    ) throws IOException, TransportException;

    public abstract void sendResponse(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final TransportResponse response,
        final boolean compress,
        final boolean isHandshake
    ) throws IOException;

    public abstract void sendErrorResponse(
        final Version nodeVersion,
        final Set<String> features,
        final TcpChannel channel,
        final long requestId,
        final String action,
        final Exception error
    ) throws IOException;
}

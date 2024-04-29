/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import java.io.IOException;

/**
 * Interface for message handlers based on transport protocol.
 *
 * @opensearch.internal
 */
public interface ProtocolMessageHandler {

    public void messageReceived(
        TcpChannel channel,
        ProtocolInboundMessage message,
        long startTime,
        long slowLogThresholdMs,
        TransportMessageListener messageListener
    ) throws IOException;

    public void setMessageListener(TransportMessageListener listener);
}

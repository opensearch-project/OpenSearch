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

    /**
     * Handles the message received on the channel.
     * @param channel the channel on which the message was received
     * @param message the message received
     * @param startTime the start time
     * @param slowLogThresholdMs the threshold for slow logs
     * @param messageListener the message listener
     */
    public void messageReceived(
        TcpChannel channel,
        ProtocolInboundMessage message,
        long startTime,
        long slowLogThresholdMs,
        TransportMessageListener messageListener
    ) throws IOException;

    /**
     * Sets the message listener to be used by the handler.
     * @param listener the message listener
     */
    public void setMessageListener(TransportMessageListener listener);
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

/**
 * Base class for inbound data as a message.
 * Different implementations are used for different protocols.
 *
 * @opensearch.internal
 */
public interface BaseInboundMessage {

    /**
     * The protocol used to encode this message
     */
    enum Protocol {
        DEFAULT,
        PROTOBUF,
    }

    /**
     * @return the protocol used to encode this message
     */
    public Protocol getProtocol();

    /**
     * Set the protocol used to encode this message
     */
    public void setProtocol();
}

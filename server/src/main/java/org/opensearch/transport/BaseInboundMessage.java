/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

public interface BaseInboundMessage {

    enum Protocol {
        DEFAULT,
        PROTOBUF,
    }

    /**
     * @return the protocol used to encode this message
     */
    public Protocol getProtocol();

    /**
     * set the protocol used to encode this message
     */
    public void setProtocol();
}

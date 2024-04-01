/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Base class for inbound data as a message.
 * Different implementations are used for different protocols.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public interface ProtocolInboundMessage {

    /**
     * The protocol used to encode this message
     */
    static String NATIVE_PROTOCOL = "native";
    static String PROTOBUF_PROTOCOL = "protobuf";

    /**
     * @return the protocol used to encode this message
     */
    default public String getProtocol() {
        return "native";
    }
}

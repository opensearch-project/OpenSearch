/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

/**
 * Enumeration of transport protocols.
 */
enum TransportProtocol {
    /**
     * The original, hand-rolled binary protocol used for node-to-node
     * communication. Message schemas are defined implicitly in code using the
     * StreamInput and StreamOutput classes to parse and generate binary data.
     */
    NATIVE;

    public static TransportProtocol fromBytes(byte b1, byte b2) {
        if (b1 == 'E' && b2 == 'S') {
            return NATIVE;
        }

        throw new IllegalArgumentException("Unknown transport protocol: [" + b1 + ", " + b2 + "]");
    }
}

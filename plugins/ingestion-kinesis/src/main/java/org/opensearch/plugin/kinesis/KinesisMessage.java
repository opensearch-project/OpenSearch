/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kinesis;

import org.opensearch.index.Message;

/**
 * Kinesis message
 */
public class KinesisMessage implements Message<byte[]> {
    private final byte[] payload;

    /**
     * Constructor
     * @param payload the payload of the message
     */
    public KinesisMessage(byte[] payload) {
        this.payload = payload;
    }

    @Override
    public byte[] getPayload() {
        return payload;
    }
}

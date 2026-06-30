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
    private final Long timestamp;

    /**
     * Constructor
     * @param payload the payload of the message
     * @param timestamp the timestamp of the message in milliseconds
     */
    public KinesisMessage(byte[] payload, Long timestamp) {
        this.payload = payload;
        this.timestamp = timestamp;
    }

    @Override
    public byte[] getPayload() {
        return payload;
    }

    @Override
    public Long getTimestamp() {
        return timestamp;
    }
}

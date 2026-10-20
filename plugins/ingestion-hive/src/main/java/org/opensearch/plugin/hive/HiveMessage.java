/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import org.opensearch.index.Message;

/**
 * A message representing a single row read from a Hive table data file.
 * The payload is the row serialized as a JSON byte array.
 */
public class HiveMessage implements Message<byte[]> {

    private final byte[] payload;
    private final Long timestamp;

    /**
     * Creates a new HiveMessage.
     *
     * @param payload the row data as a JSON byte array
     * @param timestamp the message timestamp in milliseconds, or null
     */
    public HiveMessage(byte[] payload, Long timestamp) {
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

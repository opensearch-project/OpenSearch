/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.fs;

import org.opensearch.index.Message;

/**
 * Message abstraction for file-based ingestion.
 */
public class FileMessage implements Message<byte[]> {
    private final byte[] payload;
    private final Long timestamp;

    /**
     * Create a file message.
     * @param payload Line contents from the file, as a byte array.
     * @param timestamp Millisecond timestamp for when the line was read.
     */
    public FileMessage(byte[] payload, Long timestamp) {
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

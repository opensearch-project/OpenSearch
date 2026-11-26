/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.index.Message;

/**
 * A single framed record read from object storage, carrying the raw record bytes and the time at which it was read.
 * Mirrors {@code FileMessage} from the ingestion-fs plugin: the payload is an unparsed {@code byte[]} that the
 * existing mapper path decodes downstream.
 */
public class ObjectStoreMessage implements Message<byte[]> {

    private final byte[] payload;
    private final Long timestamp;

    /**
     * Creates a message.
     *
     * @param payload   the raw record bytes; may be {@code null}, mirroring {@code FileMessage}
     * @param timestamp the time the record was read, in milliseconds since epoch
     */
    public ObjectStoreMessage(byte[] payload, Long timestamp) {
        this.payload = payload == null ? null : payload.clone();
        this.timestamp = timestamp;
    }

    @Override
    public byte[] getPayload() {
        return payload == null ? null : payload.clone();
    }

    @Override
    public Long getTimestamp() {
        return timestamp;
    }
}

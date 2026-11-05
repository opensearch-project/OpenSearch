/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.Message;

/**
 * Message that will be handled by {@link
 * org.opensearch.indices.pollingingest.mappers.DefaultIngestionMessageMapper}
 */
public class ArrowMessage implements Message<byte[]> {

    private final byte[] payload;
    private final Long ts;

    /**
     * Ctor from payload and timestamp
     *
     * @param payload the serialized document payload
     * @param ts the timestamp associated with this message
     */
    public ArrowMessage(byte[] payload, Long ts) {
        this.payload = payload;
        this.ts = ts;
    }

    /**
     * Ctor from a XContentBuilder and timestamp
     *
     * <p>TODO: here we convert from XContent to bytes and then later convert it back in {@link
     * org.opensearch.indices.pollingingest.mappers.DefaultIngestionMessageMapper},
     * if we're able to have a new message mapper impl, we can avoid those redundant
     * conversions
     *
     * @param xContentBuilder builder holding the already-serialized document content
     * @param ts the timestamp associated with this message
     */
    public ArrowMessage(XContentBuilder xContentBuilder, Long ts) {
        this.payload = BytesReference.toBytes(BytesReference.bytes(xContentBuilder));
        this.ts = ts;
    }

    @Override
    public byte[] getPayload() {
        return payload;
    }

    @Override
    public Long getTimestamp() {
        return ts;
    }
}

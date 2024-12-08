/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import java.nio.ByteBuffer;

public class KafkaOffset implements IngestionShardPointer {

    private final long offset;

    public KafkaOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(offset);
        return buffer.array();
    }

    @Override
    public KafkaOffset deserialize(byte[] serialized) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(serialized);
        buffer.flip();
        return new KafkaOffset(buffer.getLong());
    }


    public long getOffset() {
        return offset;
    }
}

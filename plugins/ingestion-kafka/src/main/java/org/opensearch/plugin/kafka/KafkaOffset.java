/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.index.IngestionShardPointer;

import java.nio.ByteBuffer;

public class KafkaOffset implements IngestionShardPointer {

    private final long offset;

    public KafkaOffset(long offset) {
        assert offset >= 0;
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

    @Override
    public String asString() {
        return String.valueOf(offset);
    }

    @Override
    public long toSequenceNumber() {
        return offset;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "KafkaOffset{" +
            "offset=" + offset +
            '}';
    }

    @Override
    public int compareTo(IngestionShardPointer o) {
        if (o == null) {
            throw new IllegalArgumentException("the pointer is null");
        }
        if (!(o instanceof KafkaOffset)) {
            throw new IllegalArgumentException("the pointer is of type " + o.getClass() + " and not KafkaOffset");
        }
        KafkaOffset other = (KafkaOffset) o;
        return Long.compare(offset, other.offset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaOffset that = (KafkaOffset) o;
        return offset == that.offset;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(offset);
    }
}

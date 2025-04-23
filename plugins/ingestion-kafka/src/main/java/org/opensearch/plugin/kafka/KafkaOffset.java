/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.opensearch.index.IngestionShardPointer;

import java.nio.ByteBuffer;

/**
 * Kafka offset.
 */
public class KafkaOffset implements IngestionShardPointer {

    private final long offset;

    /**
     * Constructor
     * @param offset the offset
     */
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
    public String asString() {
        return String.valueOf(offset);
    }

    @Override
    public Field asPointField(String fieldName) {
        return new LongPoint(fieldName, offset);
    }

    @Override
    public Query newRangeQueryGreaterThan(String fieldName) {
        return LongPoint.newRangeQuery(fieldName, offset, Long.MAX_VALUE);
    }

    /**
     * Get the offset
     * @return the offset
     */
    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "KafkaOffset{" + "offset=" + offset + '}';
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

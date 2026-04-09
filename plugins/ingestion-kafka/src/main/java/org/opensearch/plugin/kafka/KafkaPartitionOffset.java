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
import org.opensearch.index.PartitionAwarePointer;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A partition-aware Kafka offset that encodes both the partition ID and offset.
 * Extends {@link KafkaOffset} so it can be used wherever KafkaOffset is expected,
 * while adding partition information via {@link PartitionAwarePointer}.
 */
public class KafkaPartitionOffset extends KafkaOffset implements PartitionAwarePointer {

    private final int partition;

    /**
     * Constructor
     * @param partition the Kafka partition ID
     * @param offset the offset within the partition
     */
    public KafkaPartitionOffset(int partition, long offset) {
        super(offset);
        assert partition >= 0 : "partition must be non-negative";
        this.partition = partition;
    }

    @Override
    public int getPartition() {
        return partition;
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + Long.BYTES);
        buffer.putInt(partition);
        buffer.putLong(getOffset());
        return buffer.array();
    }

    /**
     * Returns string representation in "partition:offset" format (e.g., "3:42").
     * The parser in {@link KafkaConsumerFactory#parsePointerFromString(String)} detects the ":"
     * to distinguish this format from the legacy plain offset format.
     */
    @Override
    public String asString() {
        return partition + ":" + getOffset();
    }

    @Override
    public Field asPointField(String fieldName) {
        // Encode as a single long for Lucene point indexing.
        // Partition is stored in upper 16 bits (max 65,535 partitions),
        // offset in lower 48 bits (max ~281 trillion).
        // This preserves sort order within a partition.
        // Note: Kafka topics exceeding 65,535 partitions will produce incorrect encoding.
        long encoded = ((long) partition << 48) | (getOffset() & 0x0000FFFFFFFFFFFFL);
        return new LongPoint(fieldName, encoded);
    }

    @Override
    public Query newRangeQueryGreaterThan(String fieldName) {
        // Scope the range query to the SAME partition — don't match offsets from other partitions.
        long lower = ((long) partition << 48) | (getOffset() & 0x0000FFFFFFFFFFFFL);
        long upper = ((long) partition << 48) | 0x0000FFFFFFFFFFFFL;
        return LongPoint.newRangeQuery(fieldName, lower, upper);
    }

    /**
     * Compares by partition first, then by offset within the same partition.
     * Cross-partition comparison is ordered by partition ID — this is meaningful for checkpoint
     * aggregation (min across partitions) but does NOT imply temporal ordering across partitions.
     */
    @Override
    public int compareTo(IngestionShardPointer o) {
        if (o == null) {
            throw new IllegalArgumentException("the pointer is null");
        }
        if (o instanceof KafkaPartitionOffset other) {
            int cmp = Integer.compare(partition, other.partition);
            return cmp != 0 ? cmp : Long.compare(getOffset(), other.getOffset());
        }
        if (o instanceof KafkaOffset other) {
            // Cross-type comparison: treat KafkaOffset as partition -1 so it sorts before all partition offsets
            return 1; // KafkaPartitionOffset always > KafkaOffset
        }
        throw new IllegalArgumentException("Cannot compare KafkaPartitionOffset with " + o.getClass().getSimpleName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaPartitionOffset that = (KafkaPartitionOffset) o;
        return partition == that.partition && getOffset() == that.getOffset();
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, getOffset());
    }

    @Override
    public String toString() {
        return "KafkaPartitionOffset{partition=" + partition + ", offset=" + getOffset() + '}';
    }
}

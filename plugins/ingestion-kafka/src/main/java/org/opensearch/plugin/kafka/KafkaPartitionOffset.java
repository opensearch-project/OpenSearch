/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.SourcePartitionAwarePointer;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A partition-aware Kafka offset that encodes both the partition ID and offset.
 * Extends {@link KafkaOffset} so it can be used wherever KafkaOffset is expected,
 * while adding partition information via {@link SourcePartitionAwarePointer}.
 *
 * <p><b>TODO: partition-aware Lucene point field / range query.</b>
 * This class intentionally inherits {@link KafkaOffset#asPointField(String)} and
 * {@link KafkaOffset#newRangeQueryGreaterThan(String)}, so the indexed Lucene
 * {@code _offset} field carries only the raw offset (no partition).
 *
 * <p>This is not needed per se right now because:
 * <ul>
 *   <li>{@code newRangeQueryGreaterThan} has no production callers in the recovery path.
 *       {@code IngestionEngine} recovers from per-partition checkpoints in Lucene commit
 *       user data ({@code batch_start_p{N}}) and re-consumes from the source. Document-level
 *       dedup relies on {@code _id}-based overwrite via Lucene's term-level update semantics,
 *       not offset range queries.</li>
 *   <li>Partition info is preserved where it is necessary: {@link #asString()} (stored field
 *       and persisted commit data), {@link #serialize()}, {@link #compareTo}, equals/hashCode,
 *       and {@link #getSourcePartition()} for in-memory per-partition checkpoint tracking.</li>
 * </ul>
 *
 * <p>When this becomes needed (e.g., per-partition offset-range dedup as a fallback to
 * {@code _id} overwrite, partition-scoped diagnostic queries, or post-recovery cleanup),
 * the right design is two separate point fields.
 */
public class KafkaPartitionOffset extends KafkaOffset implements SourcePartitionAwarePointer {

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
    public int getSourcePartition() {
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
     * to distinguish this format from the simple plain offset format.
     */
    @Override
    public String asString() {
        return partition + ":" + getOffset();
    }

    /**
     * Compares by partition first, then by offset within the same partition.
     * Cross-partition comparison is ordered by partition ID, which is meaningful for checkpoint
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
        if (o instanceof KafkaOffset) {
            // Cross-type comparison: KafkaPartitionOffset always sorts after legacy KafkaOffset.
            // Matches the inverse in KafkaOffset.compareTo(KafkaPartitionOffset) which returns -1.
            return 1;
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

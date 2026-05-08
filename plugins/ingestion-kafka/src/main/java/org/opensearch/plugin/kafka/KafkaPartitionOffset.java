/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.lucene.search.Query;
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
 * The write side ({@link #asPointField(String)}) intentionally inherits {@link KafkaOffset}'s
 * implementation, so the indexed Lucene {@code _offset} field carries only the raw offset (no
 * partition). Every multi-partition document writes this field on indexing — but it cannot be
 * queried correctly per-partition until a proper per-partition field design lands.
 *
 * <p>The companion read method {@link #newRangeQueryGreaterThan(String)} is overridden to
 * <b>throw</b> {@code UnsupportedOperationException} rather than silently returning the inherited
 * cross-partition query (which would match documents from ANY partition with offset greater than
 * this pointer's offset). The asymmetry is intentional: writing a placeholder field is harmless
 * and required by the indexing path, but reading it via the standard pointer query API would
 * silently produce wrong results.
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
 * the right design is two separate point fields — an {@code IntPoint} for partition + the
 * existing {@code LongPoint} for offset, combined via a {@code BooleanQuery}. That requires
 * extending the {@link IngestionShardPointer} contract to return multiple fields, which should
 * be done deliberately at the point a real caller is wired in.
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

    /**
     * Parses a {@code "partition:offset"} string (e.g., {@code "3:42"}) into a
     * {@link KafkaPartitionOffset}. Inverse of {@link #asString()}.
     *
     * @param s the {@code "partition:offset"} string
     * @return the parsed {@link KafkaPartitionOffset}
     * @throws IllegalArgumentException if {@code s} is not exactly two non-empty parts separated
     *                                  by {@code ":"}, or if either part is not a valid number
     */
    public static KafkaPartitionOffset parse(String s) {
        // split(":", -1) preserves trailing empty fields so "3:" yields ["3", ""] (length 2)
        String[] parts = s.split(":", -1);
        if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
            throw new IllegalArgumentException(
                "Invalid multi-partition pointer format. Expected 'partition:offset' (e.g., '3:42'), got: " + s
            );
        }
        try {
            return new KafkaPartitionOffset(Integer.parseInt(parts[0]), Long.parseLong(parts[1]));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid multi-partition pointer format. Expected numeric 'partition:offset' (e.g., '3:42'), got: " + s,
                e
            );
        }
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
     * Throws {@code UnsupportedOperationException} — see class-level Javadoc. Inheriting
     * {@link KafkaOffset#newRangeQueryGreaterThan(String)} would silently match documents from
     * ANY partition with an offset greater than this pointer's offset, since the indexed
     * {@code _offset} field carries no partition info. Per-partition queries need a different
     * field design (two separate indexed fields combined via {@code BooleanQuery}); throwing
     * here forces future callers to recognize the multi-partition case rather than getting
     * cross-partition matches by accident. No production caller today.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public Query newRangeQueryGreaterThan(String fieldName) {
        throw new UnsupportedOperationException(
            "newRangeQueryGreaterThan() inherited from KafkaOffset would match documents from ANY "
                + "partition with offset greater than this pointer's offset, because the indexed "
                + "_offset field carries no partition info. For per-partition dedup queries, the right "
                + "design is two separate indexed fields (IntPoint for partition + LongPoint for offset) "
                + "combined via BooleanQuery — requires extending the IngestionShardPointer interface."
        );
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.opensearch.index.IngestionShardPointer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Pointer representing a position within a Hive table.
 * Encodes partition name, file path, row index, and a monotonic sequence number.
 * The sequence number is used for ordering and checkpoint recovery.
 */
public class HivePointer implements IngestionShardPointer {

    private final String partitionName;
    private final String filePath;
    private final long rowIndex;
    private final long sequenceNumber;

    /**
     * Creates a new HivePointer.
     *
     * @param partitionName the Hive partition name (e.g., {@code dt=2026-04-15})
     * @param filePath the full path to the data file
     * @param rowIndex the row offset within the file
     * @param sequenceNumber monotonically increasing counter for ordering
     */
    public HivePointer(String partitionName, String filePath, long rowIndex, long sequenceNumber) {
        this.partitionName = partitionName;
        this.filePath = filePath;
        this.rowIndex = rowIndex;
        this.sequenceNumber = sequenceNumber;
    }

    /** Returns the partition name. */
    public String getPartitionName() {
        return partitionName;
    }

    /** Returns the data file path. */
    public String getFilePath() {
        return filePath;
    }

    /** Returns the row index within the file. */
    public long getRowIndex() {
        return rowIndex;
    }

    /** Returns the sequence number. */
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String asString() {
        return partitionName + "|" + filePath + "|" + rowIndex + "|" + sequenceNumber;
    }

    /**
     * Deserializes a HivePointer from its string representation.
     *
     * @param s the serialized pointer string
     * @return the deserialized HivePointer
     */
    public static HivePointer fromString(String s) {
        String[] parts = s.split("\\|", 4);
        return new HivePointer(parts[0], parts[1], Long.parseLong(parts[2]), Long.parseLong(parts[3]));
    }

    @Override
    public byte[] serialize() {
        byte[] str = asString().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + str.length);
        buffer.putLong(sequenceNumber);
        buffer.put(str);
        return buffer.array();
    }

    @Override
    public Field asPointField(String fieldName) {
        return new LongPoint(fieldName, sequenceNumber);
    }

    @Override
    public Query newRangeQueryGreaterThan(String fieldName) {
        return LongPoint.newRangeQuery(fieldName, sequenceNumber + 1, Long.MAX_VALUE);
    }

    @Override
    public int compareTo(IngestionShardPointer o) {
        if (!(o instanceof HivePointer other)) {
            throw new IllegalArgumentException("Cannot compare with " + o.getClass());
        }
        return Long.compare(sequenceNumber, other.sequenceNumber);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HivePointer other)) return false;
        return sequenceNumber == other.sequenceNumber
            && rowIndex == other.rowIndex
            && Objects.equals(partitionName, other.partitionName)
            && Objects.equals(filePath, other.filePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionName, filePath, rowIndex, sequenceNumber);
    }

    @Override
    public String toString() {
        return "HivePointer{" + asString() + "}";
    }
}

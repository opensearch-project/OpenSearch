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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IngestionShardPointer;

import java.io.IOException;
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
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("p", partitionName);
            builder.field("f", filePath);
            builder.field("r", rowIndex);
            builder.field("s", sequenceNumber);
            builder.endObject();
            return builder.toString();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize HivePointer", e);
        }
    }

    /**
     * Deserializes a HivePointer from its JSON string representation.
     *
     * @param s the serialized pointer string
     * @return the deserialized HivePointer
     */
    public static HivePointer fromString(String s) {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, s)) {
            String partition = null;
            String file = null;
            long row = 0;
            long seq = 0;
            parser.nextToken(); // START_OBJECT
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String field = parser.currentName();
                parser.nextToken();
                switch (field) {
                    case "p":
                        partition = parser.text();
                        break;
                    case "f":
                        file = parser.text();
                        break;
                    case "r":
                        row = parser.longValue();
                        break;
                    case "s":
                        seq = parser.longValue();
                        break;
                    default:
                        break;
                }
            }
            return new HivePointer(partition, file, row, seq);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse HivePointer: " + s, e);
        }
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import java.nio.ByteBuffer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.opensearch.index.IngestionShardPointer;

/**
 * Offset for arrow dataset, for now it will be a simple row number (long) to point to a row number
 * in arrow dataset
 */
public class ArrowOffset implements IngestionShardPointer {
    private final long offset;

    /**
     * Ctor
     *
     * @param offset row number
     */
    public ArrowOffset(long offset) {
        this.offset = offset;
    }

    /**
     * Parse from string, the format is expected to be the same as what returned from {@link
     * #asString()}
     *
     * @param s the string to parse, as previously produced by {@link #asString()}
     */
    public static ArrowOffset fromString(String s) {
        return new ArrowOffset(Long.parseLong(s));
    }

    /** Get the underlying offset */
    public long getOffset() {
        return offset;
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

    /**
     * Rendered as-is into the {@code batch_start_pointer} field of the {@code GET
     * /{index}/ingestion/_state} response, which stringifies the pointer through {@code toString()}
     * rather than {@link #asString()}. This is deliberately different from {@link #asString()} for
     * a more human-readable output
     */
    @Override
    public String toString() {
        return "ArrowOffset{offset=" + offset + "}";
    }

    @Override
    public Field asPointField(String fieldName) {
        return new LongPoint(fieldName, offset);
    }

    @Override
    public Query newRangeQueryGreaterThan(String fieldName) {
        return LongPoint.newRangeQuery(fieldName, offset, Long.MAX_VALUE);
    }

    @Override
    public int compareTo(IngestionShardPointer o) {
        if (o == null) {
            throw new IllegalArgumentException("the pointer is null");
        }
        if (!(o instanceof ArrowOffset other)) {
            throw new IllegalArgumentException(
                    "the pointer is of type " + o.getClass() + " and not ArrowOffset");
        }
        return Long.compare(offset, other.offset);
    }
}

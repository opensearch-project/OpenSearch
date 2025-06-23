/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.fs;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.opensearch.index.IngestionShardPointer;

import java.nio.ByteBuffer;

/**
 * Offset for a file-based ingestion source (line number).
 */
public class FileOffset implements IngestionShardPointer {

    private final long line;

    /**
     * Create a new file offset based on line number.
     * @param line line number (offset)
     */
    public FileOffset(long line) {
        assert line >= 0;
        this.line = line;
    }

    /**
     * Returns the line number (offset).
     */
    public long getLine() {
        return line;
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(line);
        return buffer.array();
    }

    @Override
    public String asString() {
        return Long.toString(line);
    }

    @Override
    public Field asPointField(String fieldName) {
        return new LongPoint(fieldName, line);
    }

    @Override
    public Query newRangeQueryGreaterThan(String fieldName) {
        return LongPoint.newRangeQuery(fieldName, line, Long.MAX_VALUE);
    }

    @Override
    public int compareTo(IngestionShardPointer o) {
        if (o == null || !(o instanceof FileOffset)) {
            throw new IllegalArgumentException("Incompatible pointer type: " + (o == null ? "null" : o.getClass()));
        }
        return Long.compare(this.line, ((FileOffset) o).line);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FileOffset)) return false;
        return this.line == ((FileOffset) o).line;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(line);
    }

    @Override
    public String toString() {
        return "FileOffset{" + "line=" + line + '}';
    }
}

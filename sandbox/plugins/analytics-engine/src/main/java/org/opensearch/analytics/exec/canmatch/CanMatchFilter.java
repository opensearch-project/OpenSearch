/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializable filter predicate for can-match evaluation. Represents a simple
 * range check on a single column: {@code column IN [minValue, maxValue]}.
 *
 * <p>Wire format for a single filter: {@code string column, long min, long max}.
 * Wire format for a list (see {@link #listToBytes} / {@link #listFromBytes}):
 * {@code vInt count, repeated single-filter bytes}.
 *
 * <p>This is intentionally simple — covers the most impactful can-match cases
 * (time-range queries, numeric range filters). Complex predicates (AND/OR trees,
 * string predicates) can be added later by extending this to a union type.
 */
public class CanMatchFilter implements Writeable {

    private static final Logger LOGGER = LogManager.getLogger(CanMatchFilter.class);

    /**
     * Wire-format version for the list serialization. Bump when the byte layout of
     * {@link #listToBytes(List)} changes. Readers seeing an unknown version fail open
     * (return empty list) so a newer coordinator writing a future shape doesn't blow
     * up a data node mid-upgrade — pruning is lost, results stay correct.
     */
    private static final int LIST_FORMAT_VERSION = 1;

    private final String columnName;
    private final long minValue;
    private final long maxValue;

    public CanMatchFilter(String columnName, long minValue, long maxValue) {
        this.columnName = columnName;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    public CanMatchFilter(StreamInput in) throws IOException {
        this.columnName = in.readString();
        this.minValue = in.readLong();
        this.maxValue = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(columnName);
        out.writeLong(minValue);
        out.writeLong(maxValue);
    }

    public String getColumnName() {
        return columnName;
    }

    public long getMinValue() {
        return minValue;
    }

    public long getMaxValue() {
        return maxValue;
    }

    /** Serializes a list of filters to a single byte buffer. Empty list → zero-length byte[]. */
    public static byte[] listToBytes(List<CanMatchFilter> filters) throws IOException {
        if (filters == null || filters.isEmpty()) {
            return new byte[0];
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(LIST_FORMAT_VERSION);
            out.writeVInt(filters.size());
            for (CanMatchFilter f : filters) {
                f.writeTo(out);
            }
            return java.util.Arrays.copyOf(out.bytes().toBytesRef().bytes, out.bytes().length());
        }
    }

    /** Deserializes a list previously produced by {@link #listToBytes}. Zero-length input → empty list. */
    public static List<CanMatchFilter> listFromBytes(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return List.of();
        }
        try (StreamInput in = new BytesArray(bytes).streamInput()) {
            int version = in.readVInt();
            if (version != LIST_FORMAT_VERSION) {
                LOGGER.warn("unknown CanMatchFilter wire-format version {} (expected {}); failing open", version, LIST_FORMAT_VERSION);
                return List.of();
            }
            int count = in.readVInt();
            List<CanMatchFilter> filters = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                filters.add(new CanMatchFilter(in));
            }
            return filters;
        }
    }
}

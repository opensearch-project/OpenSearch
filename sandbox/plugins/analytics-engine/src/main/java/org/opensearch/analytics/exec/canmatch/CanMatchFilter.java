/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Serializable filter predicate for can-match evaluation. Represents a simple
 * range check on a single column: {@code column IN [minValue, maxValue]}.
 *
 * <p>This is intentionally simple — covers the most impactful can-match cases
 * (time-range queries, numeric range filters). Complex predicates (AND/OR trees,
 * string predicates) can be added later by extending this to a union type.
 */
public class CanMatchFilter implements Writeable {

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

    /**
     * Serialize to bytes for transport.
     */
    public byte[] toBytes() throws IOException {
        try (var out = new org.opensearch.common.io.stream.BytesStreamOutput()) {
            writeTo(out);
            return out.bytes().toBytesRef().bytes;
        }
    }

    /**
     * Deserialize from bytes.
     */
    public static CanMatchFilter fromBytes(byte[] bytes) throws IOException {
        try (var in = new org.opensearch.common.io.stream.BytesStreamOutput()) {
            // Use StreamInput from bytes
        }
        var ref = new org.opensearch.core.common.bytes.BytesArray(bytes);
        try (var in = ref.streamInput()) {
            return new CanMatchFilter(in);
        }
    }
}

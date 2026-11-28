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

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Membership filter on an integer/date column: the shard is kept only if some
 * row group could hold one of {@code values}.
 *
 * <p>Strictly stronger than a {@link LongRange} over the same values. A range can
 * only prove a row group lies entirely outside {@code [min, max]}; a set also
 * excludes row groups that fall in the gaps between candidates. That difference
 * is what makes a runtime filter worth shipping: a join key set drawn from a
 * filtered build side is usually sparse, so most of its envelope is gap.
 *
 * <p>Values are sorted and de-duplicated on construction, which makes the wire
 * form deterministic and lets the data node binary-search each row group's
 * {@code [min, max]} instead of scanning the set.
 *
 * <p>An empty set is rejected rather than treated as "prune everything". An empty
 * build side does mean an inner join yields nothing, but that is a whole-query
 * short circuit and deserves to be an explicit decision by the caller, not an
 * emergent consequence of a filter carrying no values — which is also what a
 * failed extraction looks like.
 *
 * @opensearch.internal
 */
public record LongSet(String column, long[] values) implements CanMatchFilter {

    /** Type string used in wire serialization. */
    public static final String TYPE = "LongSet";

    public LongSet {
        Objects.requireNonNull(column, "column");
        Objects.requireNonNull(values, "values");
        values = Arrays.stream(values).sorted().distinct().toArray();
        if (values.length == 0) {
            throw new IllegalArgumentException("LongSet requires at least one value; column=" + column);
        }
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public void writeBody(StreamOutput out) throws IOException {
        out.writeString(column);
        out.writeVInt(values.length);
        for (long value : values) {
            out.writeLong(value);
        }
    }

    /** Deserialize the body fields (type string already consumed by the codec). */
    public static LongSet readBody(StreamInput in) throws IOException {
        String column = in.readString();
        int count = in.readVInt();
        long[] values = new long[count];
        for (int i = 0; i < count; i++) {
            values[i] = in.readLong();
        }
        return new LongSet(column, values);
    }

    /** Smallest candidate. Cheap because {@link #values} is sorted. */
    public long min() {
        return values[0];
    }

    /** Largest candidate. */
    public long max() {
        return values[values.length - 1];
    }

    // A record whose component is an array gets identity-based equals/hashCode from
    // the compiler, which is wrong for a value carrier. Override all three.

    @Override
    public boolean equals(Object other) {
        return other instanceof LongSet(String otherColumn, long[] otherValues)
            && column.equals(otherColumn)
            && Arrays.equals(values, otherValues);
    }

    @Override
    public int hashCode() {
        return 31 * column.hashCode() + Arrays.hashCode(values);
    }

    @Override
    public String toString() {
        return "LongSet[column=" + column + ", size=" + values.length + ", min=" + min() + ", max=" + max() + "]";
    }
}

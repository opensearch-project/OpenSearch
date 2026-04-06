/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents the range of values for a long field (typically {@code @timestamp}) across an entire index.
 * <p>
 * This is stored in {@link IndexMetadata} as part of cluster state, enabling the coordinating node
 * to skip indices whose timestamp range does not overlap a query's time range — without sending
 * any network {@code can_match} probes to data nodes.
 * <p>
 * Three states are possible:
 * <ul>
 *   <li>{@link #UNKNOWN} — the range has not been computed yet. Queries must fall through to
 *       shard-level {@code can_match}.</li>
 *   <li>{@link #EMPTY} — the field does not exist in the index mapping or the index has no documents.
 *       The index is guaranteed to not match any range query on this field.</li>
 *   <li>A concrete range with known {@code min} and {@code max} values.</li>
 * </ul>
 *
 * @opensearch.experimental
 */
public final class IndexLongFieldRange implements Writeable, ToXContentFragment {

    private static final String KEY_MIN = "min";
    private static final String KEY_MAX = "max";
    private static final String KEY_UNKNOWN = "unknown";
    private static final String KEY_EMPTY = "empty";

    private static final byte SERIAL_UNKNOWN = 0;
    private static final byte SERIAL_EMPTY = 1;
    private static final byte SERIAL_RANGE = 2;

    /**
     * The range is not yet known. This is the default for all indices until the range is populated.
     * Queries targeting this index must fall through to the existing shard-level {@code can_match} path.
     */
    public static final IndexLongFieldRange UNKNOWN = new IndexLongFieldRange(true, false, Long.MIN_VALUE, Long.MAX_VALUE);

    /**
     * The field does not exist in this index or the index contains no documents.
     * Any range query on this field is guaranteed to not match.
     */
    public static final IndexLongFieldRange EMPTY = new IndexLongFieldRange(false, true, Long.MIN_VALUE, Long.MAX_VALUE);

    private final boolean unknown;
    private final boolean empty;
    private final long min;
    private final long max;

    private IndexLongFieldRange(boolean unknown, boolean empty, long min, long max) {
        this.unknown = unknown;
        this.empty = empty;
        this.min = min;
        this.max = max;
    }

    /**
     * Creates a range with known min and max values.
     */
    public static IndexLongFieldRange of(long min, long max) {
        if (min > max) {
            throw new IllegalArgumentException("min [" + min + "] must be <= max [" + max + "]");
        }
        return new IndexLongFieldRange(false, false, min, max);
    }

    /**
     * Returns {@code true} if the range is not yet known.
     */
    public boolean isUnknown() {
        return unknown;
    }

    /**
     * Returns {@code true} if the field does not exist or the index has no documents.
     */
    public boolean isEmpty() {
        return empty;
    }

    /**
     * Returns the minimum value, only meaningful when neither {@link #isUnknown()} nor {@link #isEmpty()}.
     */
    public long getMin() {
        return min;
    }

    /**
     * Returns the maximum value, only meaningful when neither {@link #isUnknown()} nor {@link #isEmpty()}.
     */
    public long getMax() {
        return max;
    }

    /**
     * Determines the relation between this index's field range and the given query range.
     *
     * @param queryMin the lower bound of the query range (inclusive)
     * @param queryMax the upper bound of the query range (inclusive)
     * @return the relation
     */
    public Relation relation(long queryMin, long queryMax) {
        if (unknown) {
            return Relation.UNKNOWN;
        }
        if (empty) {
            return Relation.DISJOINT;
        }
        if (max < queryMin || min > queryMax) {
            return Relation.DISJOINT;
        }
        if (min >= queryMin && max <= queryMax) {
            return Relation.WITHIN;
        }
        return Relation.INTERSECTS;
    }

    /**
     * Returns a new range that extends this range to include the given value.
     */
    public IndexLongFieldRange extendWithValue(long value) {
        if (unknown || empty) {
            return IndexLongFieldRange.of(value, value);
        }
        return IndexLongFieldRange.of(Math.min(min, value), Math.max(max, value));
    }

    /**
     * Returns a new range that is the union of this range and the given range.
     */
    public IndexLongFieldRange union(IndexLongFieldRange other) {
        if (other.unknown || this.unknown) {
            return UNKNOWN;
        }
        if (other.empty) {
            return this;
        }
        if (this.empty) {
            return other;
        }
        return IndexLongFieldRange.of(Math.min(this.min, other.min), Math.max(this.max, other.max));
    }

    // --- Serialization ---

    public IndexLongFieldRange(StreamInput in) throws IOException {
        byte type = in.readByte();
        switch (type) {
            case SERIAL_UNKNOWN:
                this.unknown = true;
                this.empty = false;
                this.min = Long.MIN_VALUE;
                this.max = Long.MAX_VALUE;
                break;
            case SERIAL_EMPTY:
                this.unknown = false;
                this.empty = true;
                this.min = Long.MIN_VALUE;
                this.max = Long.MAX_VALUE;
                break;
            case SERIAL_RANGE:
                this.unknown = false;
                this.empty = false;
                this.min = in.readLong();
                this.max = in.readLong();
                break;
            default:
                throw new IOException("Unknown IndexLongFieldRange type: " + type);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (unknown) {
            out.writeByte(SERIAL_UNKNOWN);
        } else if (empty) {
            out.writeByte(SERIAL_EMPTY);
        } else {
            out.writeByte(SERIAL_RANGE);
            out.writeLong(min);
            out.writeLong(max);
        }
    }

    // --- XContent ---

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (unknown) {
            builder.field(KEY_UNKNOWN, true);
        } else if (empty) {
            builder.field(KEY_EMPTY, true);
        } else {
            builder.field(KEY_MIN, min);
            builder.field(KEY_MAX, max);
        }
        return builder;
    }

    public static IndexLongFieldRange fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        Long min = null;
        Long max = null;
        boolean isUnknown = false;
        boolean isEmpty = false;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (KEY_MIN.equals(currentFieldName)) {
                    min = parser.longValue();
                } else if (KEY_MAX.equals(currentFieldName)) {
                    max = parser.longValue();
                } else if (KEY_UNKNOWN.equals(currentFieldName)) {
                    isUnknown = parser.booleanValue();
                } else if (KEY_EMPTY.equals(currentFieldName)) {
                    isEmpty = parser.booleanValue();
                }
            }
        }

        if (isUnknown) {
            return UNKNOWN;
        }
        if (isEmpty) {
            return EMPTY;
        }
        if (min != null && max != null) {
            return IndexLongFieldRange.of(min, max);
        }
        return UNKNOWN;
    }

    // --- Object methods ---

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexLongFieldRange that = (IndexLongFieldRange) o;
        return unknown == that.unknown && empty == that.empty && min == that.min && max == that.max;
    }

    @Override
    public int hashCode() {
        return Objects.hash(unknown, empty, min, max);
    }

    @Override
    public String toString() {
        if (unknown) {
            return "IndexLongFieldRange[UNKNOWN]";
        }
        if (empty) {
            return "IndexLongFieldRange[EMPTY]";
        }
        return "IndexLongFieldRange[" + min + "-" + max + "]";
    }

    /**
     * The relation between an index's field range and a query range.
     */
    public enum Relation {
        /** The range has not been computed; no pruning decision can be made. */
        UNKNOWN,
        /** The index's range does not overlap the query range at all. */
        DISJOINT,
        /** The index's range is entirely contained within the query range. */
        WITHIN,
        /** The index's range partially overlaps the query range. */
        INTERSECTS
    }
}

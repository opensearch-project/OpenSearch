/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Shard-wide min/max of one column, from the can-match probe. The coordinator uses it to
 * dispatch the most promising shards first for a sorted top-N query.
 *
 * <p>Bounds are a hint, never a requirement: absent bounds mean "unknown" and every
 * consumer falls back to its unoptimized path.
 *
 * <p>{@code min}/{@code max} cover the non-null values only. A column with no non-null
 * values reports no bounds rather than a made-up range.
 *
 * @param hasNulls true when the column may hold a null anywhere on the shard, or when the null
 *                 count couldn't be established. Calcite maps {@code DESC} to
 *                 {@code NULLS FIRST}, so one null outranks every real value and lands the shard
 *                 in the top-N however poor its {@code min}/{@code max} — never eliminate a shard
 *                 that reports {@code true}.
 * @param valueKind value domain the statistics came from. Travels on the wire so the
 *                  coordinator can refuse to compare bounds read at different scales
 *                  (e.g. millisecond vs nanosecond timestamps).
 *
 * @opensearch.internal
 */
public record ShardSortBounds(long min, long max, boolean hasNulls, byte valueKind) implements Writeable {

    /** Parquet INT32 column. Must match the Rust-side constant. */
    public static final byte VALUE_KIND_INT32 = 1;

    /** Parquet INT64 column with no logical annotation — a plain integer domain. */
    public static final byte VALUE_KIND_INT64 = 2;

    /** Parquet INT64 annotated {@code Timestamp(MILLIS)} — what a {@code date} field writes. */
    public static final byte VALUE_KIND_INT64_MILLIS = 3;

    /** Parquet INT64 annotated {@code Timestamp(MICROS)}. */
    public static final byte VALUE_KIND_INT64_MICROS = 4;

    /**
     * Parquet INT64 annotated {@code Timestamp(NANOS)} — what a {@code date_nanos} field writes.
     * Kept separate from {@link #VALUE_KIND_INT64_MILLIS}: same physical type, scaled 10^6 apart,
     * so one kind for both would let the coordinator compare millis against nanos.
     */
    public static final byte VALUE_KIND_INT64_NANOS = 5;

    public ShardSortBounds(StreamInput in) throws IOException {
        this(in.readLong(), in.readLong(), in.readBoolean(), in.readByte());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(min);
        out.writeLong(max);
        out.writeBoolean(hasNulls);
        out.writeByte(valueKind);
    }
}

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
 * @param valueKind physical type the statistics came from. Travels on the wire so the
 *                  coordinator can refuse to compare bounds read at different scales
 *                  (e.g. millisecond vs nanosecond timestamps).
 *
 * @opensearch.internal
 */
public record ShardSortBounds(long min, long max, byte valueKind) implements Writeable {

    /** Parquet INT32 column. Must match the Rust-side constant. */
    public static final byte VALUE_KIND_INT32 = 1;

    /** Parquet INT64 column. Must match the Rust-side constant. */
    public static final byte VALUE_KIND_INT64 = 2;

    public ShardSortBounds(StreamInput in) throws IOException {
        this(in.readLong(), in.readLong(), in.readByte());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(min);
        out.writeLong(max);
        out.writeByte(valueKind);
    }
}

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

/**
 * Range filter on an integer/date column. The data node checks whether
 * any parquet row group's min/max for this column overlaps [min, max].
 *
 * <p>Covers the dominant can-match case: timestamp range on
 * time-partitioned data, numeric range on integer fields. Timestamps
 * encode as epoch millis (long); integers fit naturally.
 *
 * @opensearch.internal
 */
public record LongRange(String column, long min, long max) implements CanMatchFilter {

    /** Type string used in wire serialization. */
    public static final String TYPE = "LongRange";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public void writeBody(StreamOutput out) throws IOException {
        out.writeString(column);
        out.writeLong(min);
        out.writeLong(max);
    }

    /** Deserialize the body fields (type string already consumed by codec). */
    public static LongRange readBody(StreamInput in) throws IOException {
        return new LongRange(in.readString(), in.readLong(), in.readLong());
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.extrasource;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Primitive longs.
 * This can be used by clients that already have long[].
 */
final class PrimitiveLongArray extends AbstractPrimitiveArray implements LongArrayValue {
    private final long[] v;

    PrimitiveLongArray(long[] v) {
        super(Objects.requireNonNull(v, "values must not be null").length);
        this.v = v;
    }

    @Override
    public long get(int i) {
        return v[i];
    }

    @Override
    public long[] asLongArray() {
        return v;
    }

    @Override
    public void writePayloadTo(StreamOutput out) throws IOException {
        out.writeLongArray(v);
    }

    static PrimitiveLongArray readBodyFrom(StreamInput in) throws IOException {
        return new PrimitiveLongArray(in.readLongArray());
    }
}

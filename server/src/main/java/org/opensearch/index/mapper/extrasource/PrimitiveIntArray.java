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
 * Primitive ints.
 * This can be used by clients that already have int[].
 */
final class PrimitiveIntArray extends AbstractPrimitiveArray implements IntArrayValue {
    private final int[] v;

    PrimitiveIntArray(int[] v) {
        super(Objects.requireNonNull(v, "values must not be null").length);
        this.v = v;
    }

    @Override
    public int get(int i) {
        return v[i];
    }

    @Override
    public int[] asIntArray() {
        return v;
    }

    @Override
    public void writePayloadTo(StreamOutput out) throws IOException {
        out.writeIntArray(v);
    }

    static PrimitiveIntArray readBodyFrom(StreamInput in) throws IOException {
        return new PrimitiveIntArray(in.readIntArray());
    }
}

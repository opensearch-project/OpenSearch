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
 * Primitive floats
 * This can be used by clients that already have float[].
 */
final class PrimitiveFloatArray extends AbstractPrimitiveArray implements FloatArrayValue {
    private final float[] v;

    public PrimitiveFloatArray(float[] v) {
        super(Objects.requireNonNull(v, "values must not be null").length);
        this.v = v;
    }

    @Override
    public float get(int i) {
        return v[i];
    }

    @Override
    public float[] asFloatArray() {
        return v;
    }

    @Override
    public void writePayloadTo(StreamOutput out) throws IOException {
        out.writeFloatArray(v);
    }

    static PrimitiveFloatArray readBodyFrom(StreamInput in) throws IOException {
        return new PrimitiveFloatArray(in.readFloatArray());
    }
}

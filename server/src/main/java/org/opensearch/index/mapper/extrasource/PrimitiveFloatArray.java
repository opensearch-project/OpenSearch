/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.extrasource;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Primitive floats
 * This can be used by clients that already have float[].
 */
public final class PrimitiveFloatArray implements FloatArrayValue {
    private final float[] v;

    public PrimitiveFloatArray(float[] v) {
        this.v = v;
    }

    @Override
    public int dimension() {
        return v.length;
    }

    @Override
    public boolean isPackedLE() {
        return false;
    }

    @Override
    public BytesReference packedBytes() {
        throw new IllegalStateException("Not packed");
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

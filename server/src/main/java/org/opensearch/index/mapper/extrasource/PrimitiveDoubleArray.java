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
 * Primitive doubles.
 * This can be used by clients that already have double[].
 */
final class PrimitiveDoubleArray extends AbstractPrimitiveArray implements DoubleArrayValue {
    private final double[] v;

    PrimitiveDoubleArray(double[] v) {
        super(Objects.requireNonNull(v, "values must not be null").length);
        this.v = v;
    }

    @Override
    public double get(int i) {
        return v[i];
    }

    @Override
    public double[] asDoubleArray() {
        return v;
    }

    @Override
    public void writePayloadTo(StreamOutput out) throws IOException {
        out.writeDoubleArray(v);
    }

    static PrimitiveDoubleArray readBodyFrom(StreamInput in) throws IOException {
        return new PrimitiveDoubleArray(in.readDoubleArray());
    }
}

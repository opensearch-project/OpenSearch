/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.extrasource;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Objects;

/**
 * Packed little-endian doubles.
 *
 * <p>Canonical storage is a {@link BytesReference} for efficient transport.</p>
 */
final class PackedDoubleArray extends AbstractPackedArray implements DoubleArrayValue {

    private volatile double[] cached;

    static PackedDoubleArray fromPackedBytes(BytesReference packed, int dimension) {
        Objects.requireNonNull(packed, "packed must not be null");
        return new PackedDoubleArray(packed, dimension, null, 0);
    }

    static PackedDoubleArray fromPackedArray(byte[] packedArray, int dimension) {
        Objects.requireNonNull(packedArray, "packedArray must not be null");
        return fromPackedArray(packedArray, 0, packedArray.length, dimension);
    }

    static PackedDoubleArray fromPackedArray(byte[] packedArray, int offset, int length, int dimension) {
        Objects.requireNonNull(packedArray, "packedArray must not be null");
        BytesReference packed = new BytesArray(packedArray, offset, length);
        return new PackedDoubleArray(packed, dimension, packedArray, offset);
    }

    private PackedDoubleArray(BytesReference packed, int dimension, byte[] bytes, int bytesOffset) {
        super(packed, dimension, bytes, bytesOffset, Double.BYTES, "double");
    }

    static PackedDoubleArray readBodyFrom(StreamInput in, int dim) throws IOException {
        return fromPackedBytes(in.readBytesReference(), dim);
    }

    @Override
    public double get(int i) {
        checkIndex(i);
        double[] v = cached;
        if (v != null) {
            return v[i];
        }
        ResolvedBytes resolved = ensureBytes();
        return decodeDoubleLEAt(resolved.bytes, resolved.offset + i * Double.BYTES);
    }

    @Override
    public double[] asDoubleArray() {
        double[] v = cached;
        if (v != null) return v;

        ResolvedBytes resolved = ensureBytes();

        v = new double[dimension];
        int p = resolved.offset;
        for (int i = 0; i < dimension; i++) {
            v[i] = decodeDoubleLEAt(resolved.bytes, p);
            p += Double.BYTES;
        }

        cached = v;
        return v;
    }
}

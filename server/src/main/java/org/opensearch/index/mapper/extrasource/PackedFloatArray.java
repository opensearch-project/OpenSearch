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
 * Packed little-endian floats.
 *
 * <p>Canonical storage is a {@link BytesReference} for efficient transport.</p>
 *
 * <p>Decoding strategy:</p>
 * <ul>
 *   <li>If created from an array (or a {@link BytesArray}), keep an array-backed view (no copy).</li>
 *   <li>Otherwise, on first access that requires decoding, materialize a compact {@code byte[]} via
 *       {@code BytesReference.toBytes(...)}. This may copy for composite/paged implementations.</li>
 * </ul>
 *
 * <p>The materialization is cached for subsequent reads; under concurrent access multiple threads may
 * transiently materialize/allocate more than once, but results are equivalent.</p>
 */

final class PackedFloatArray extends AbstractPackedArray implements FloatArrayValue {

    private volatile float[] cached;

    public static PackedFloatArray fromPackedBytes(BytesReference packed, int dimension) {
        Objects.requireNonNull(packed, "packed must not be null");
        return new PackedFloatArray(packed, dimension, null, 0);
    }

    public static PackedFloatArray fromPackedArray(byte[] packedArray, int dimension) {
        Objects.requireNonNull(packedArray, "packedArray must not be null");
        return fromPackedArray(packedArray, 0, packedArray.length, dimension);
    }

    public static PackedFloatArray fromPackedArray(byte[] packedArray, int offset, int length, int dimension) {
        Objects.requireNonNull(packedArray, "packedArray must not be null");
        BytesReference packed = new BytesArray(packedArray, offset, length);
        return new PackedFloatArray(packed, dimension, packedArray, offset);
    }

    private PackedFloatArray(BytesReference packed, int dimension, byte[] bytes, int bytesOffset) {
        super(packed, dimension, bytes, bytesOffset, Float.BYTES, "float");
    }

    static PackedFloatArray readBodyFrom(StreamInput in, int dim) throws IOException {
        return fromPackedBytes(in.readBytesReference(), dim);
    }

    @Override
    public float get(int i) {
        checkIndex(i);
        float[] v = cached;
        if (v != null) {
            return v[i];
        }
        ResolvedBytes resolved = ensureBytes();
        return decodeFloatLEAt(resolved.bytes, resolved.offset + i * Float.BYTES);
    }

    @Override
    public float[] asFloatArray() {
        float[] v = cached;
        if (v != null) return v;

        ResolvedBytes resolved = ensureBytes();

        v = new float[dimension];
        int p = resolved.offset;
        for (int i = 0; i < dimension; i++) {
            v[i] = decodeFloatLEAt(resolved.bytes, p);
            p += Float.BYTES;
        }

        cached = v;
        return v;
    }
}

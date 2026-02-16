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
import org.opensearch.core.common.io.stream.StreamOutput;

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

public final class PackedFloatArray implements FloatArrayValue {

    private final BytesReference packed;
    private final int dimension;

    // Lazily materialized backing for fast decoding
    private volatile byte[] bytes;
    private volatile int bytesOffset;

    private volatile float[] cached;

    public static PackedFloatArray fromPackedBytes(BytesReference packed, int dimension) {
        Objects.requireNonNull(packed, "packed must not be null");
        return new PackedFloatArray(packed, dimension, null, 0);
    }

    public static PackedFloatArray fromPackedArray(byte[] packedArray, int dimension) {
        return fromPackedArray(packedArray, 0, packedArray.length, dimension);
    }

    public static PackedFloatArray fromPackedArray(byte[] packedArray, int offset, int length, int dimension) {
        Objects.requireNonNull(packedArray, "packedArray must not be null");
        BytesReference packed = new BytesArray(packedArray, offset, length);
        return new PackedFloatArray(packed, dimension, packedArray, offset);
    }

    private PackedFloatArray(BytesReference packed, int dimension, byte[] bytes, int bytesOffset) {
        this.packed = packed;
        this.dimension = dimension;
        this.bytes = bytes;
        this.bytesOffset = bytesOffset;
        validate(packed.length(), dimension);
    }

    @Override
    public int dimension() {
        return dimension;
    }

    @Override
    public boolean isPackedLE() {
        return true;
    }

    @Override
    public BytesReference packedBytes() {
        return packed;
    }

    @Override
    public void writePayloadTo(StreamOutput out) throws IOException {
        out.writeVInt(dimension());
        out.writeBytesReference(packed);
    }

    static PackedFloatArray readBodyFrom(StreamInput in, int dim) throws IOException {
        return fromPackedBytes(in.readBytesReference(), dim);
    }

    private void ensureBytes() {
        if (bytes != null) {
            return;
        }

        byte[] arr;
        int off;

        // Avoid compaction/copy for the common array-backed case
        // Otherwise, compact once if needed (composite/paged/etc).
        if (packed instanceof BytesArray ba) {
            arr = ba.array();
            off = ba.offset();
        } else {
            arr = BytesReference.toBytes(packed);
            off = 0;
        }

        bytesOffset = off;
        bytes = arr;
    }

    private float decodeAt(final int p) {
        final byte[] a = bytes;
        final int bits = (a[p] & 0xFF) | ((a[p + 1] & 0xFF) << 8) | ((a[p + 2] & 0xFF) << 16) | ((a[p + 3] & 0xFF) << 24);
        return Float.intBitsToFloat(bits);
    }

    @Override
    public float get(int i) {
        if (i < 0 || i >= dimension) {
            throw new IndexOutOfBoundsException("i=" + i + " dim=" + dimension);
        }
        float[] v = cached;
        if (v != null) {
            return v[i];
        }
        ensureBytes();
        return decodeAt(bytesOffset + (i << 2));
    }

    @Override
    public float[] asFloatArray() {
        float[] v = cached;
        if (v != null) return v;

        ensureBytes();

        v = new float[dimension];
        int p = bytesOffset;
        for (int i = 0; i < dimension; i++) {
            v[i] = decodeAt(p);
            p += 4;
        }

        cached = v;
        return v;
    }

    private static void validate(int byteLen, int dim) {
        if (dim < 0) {
            throw new IllegalArgumentException("dimension must be >= 0 (got " + dim + ")");
        }
        final int expected;
        try {
            expected = Math.multiplyExact(dim, 4);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("dimension too large: " + dim, e);
        }
        if (byteLen != expected) {
            throw new IllegalArgumentException("Bad packed float length=" + byteLen + " expected=" + expected + " (dim=" + dim + ")");
        }
    }
}

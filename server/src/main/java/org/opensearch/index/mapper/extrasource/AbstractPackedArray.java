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
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Shared storage and little-endian decoding for packed primitive arrays.
 *
 * <p>Element access is intentionally left to the concrete primitive types instead of
 * being modeled with generics. Java generics would require boxed values such as
 * {@code Integer}, {@code Long}, {@code Float}, and {@code Double}; keeping
 * type-specific accessors avoids that overhead on indexing paths.</p>
 */
abstract class AbstractPackedArray {

    protected final int dimension;
    protected volatile byte[] bytes;
    protected volatile int bytesOffset;

    private final BytesReference packed;

    AbstractPackedArray(BytesReference packed, int dimension, byte[] bytes, int bytesOffset, int bytesPerElement, String valueType) {
        this.packed = Objects.requireNonNull(packed, "packed must not be null");
        this.dimension = dimension;
        this.bytes = bytes;
        this.bytesOffset = bytesOffset;
        validate(packed.length(), dimension, bytesPerElement, valueType);
    }

    public final int dimension() {
        return dimension;
    }

    public final boolean isPackedLE() {
        return true;
    }

    public final BytesReference packedBytes() {
        return packed;
    }

    public final void writePayloadTo(StreamOutput out) throws IOException {
        out.writeVInt(dimension());
        out.writeBytesReference(packed);
    }

    protected final void ensureBytes() {
        if (bytes != null) {
            return;
        }

        byte[] arr;
        int off;
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

    protected final void checkIndex(int i) {
        if (i < 0 || i >= dimension) {
            throw new IndexOutOfBoundsException("i=" + i + " dim=" + dimension);
        }
    }

    protected final int decodeIntLEAt(final int p) {
        return decodeIntLEAt(bytes, p);
    }

    private static int decodeIntLEAt(final byte[] a, final int p) {
        return (a[p] & 0xFF) | ((a[p + 1] & 0xFF) << 8) | ((a[p + 2] & 0xFF) << 16) | ((a[p + 3] & 0xFF) << 24);
    }

    protected final long decodeLongLEAt(final int p) {
        final byte[] a = bytes;
        return (decodeIntLEAt(a, p) & 0xFFFFFFFFL) | ((decodeIntLEAt(a, p + Integer.BYTES) & 0xFFFFFFFFL) << 32);
    }

    protected final float decodeFloatLEAt(final int p) {
        return Float.intBitsToFloat(decodeIntLEAt(p));
    }

    protected final double decodeDoubleLEAt(final int p) {
        return Double.longBitsToDouble(decodeLongLEAt(p));
    }

    private static void validate(int byteLen, int dim, int bytesPerElement, String valueType) {
        if (dim < 0) {
            throw new IllegalArgumentException("dimension must be >= 0 (got " + dim + ")");
        }
        final int expected;
        try {
            expected = Math.multiplyExact(dim, bytesPerElement);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("dimension too large: " + dim, e);
        }
        if (byteLen != expected) {
            throw new IllegalArgumentException(
                "Bad packed " + valueType + " length=" + byteLen + " expected=" + expected + " (dim=" + dim + ")"
            );
        }
    }
}

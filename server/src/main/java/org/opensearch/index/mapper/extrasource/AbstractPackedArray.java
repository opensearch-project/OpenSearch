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
 *
 * <p>The repeated {@code get(int)} and array materialization loops in subclasses are
 * deliberate. Extracting them into a generic or callback-based helper would add boxing
 * or per-element indirection in hot indexing paths.</p>
 */
abstract class AbstractPackedArray {

    protected static final class ResolvedBytes {
        final byte[] bytes;
        final int offset;

        private ResolvedBytes(byte[] bytes, int offset) {
            this.bytes = bytes;
            this.offset = offset;
        }
    }

    protected final int dimension;

    private final BytesReference packed;
    private volatile ResolvedBytes resolvedBytes;

    AbstractPackedArray(BytesReference packed, int dimension, byte[] bytes, int bytesOffset, int bytesPerElement, String valueType) {
        this.packed = Objects.requireNonNull(packed, "packed must not be null");
        this.dimension = dimension;
        if (bytes != null) {
            Objects.checkFromIndexSize(bytesOffset, packed.length(), bytes.length);
        }
        this.resolvedBytes = bytes == null ? null : new ResolvedBytes(bytes, bytesOffset);
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

    protected final ResolvedBytes ensureBytes() {
        ResolvedBytes view = resolvedBytes;
        if (view != null) {
            return view;
        }

        // This cache is intentionally lock-free. The expected indexing path consumes each
        // value from one thread; if a value is shared concurrently, duplicate first-time
        // resolution is harmless because all resolved views contain the same bytes.
        byte[] arr;
        int off;
        if (packed instanceof BytesArray ba) {
            arr = ba.array();
            off = ba.offset();
        } else {
            arr = BytesReference.toBytes(packed);
            off = 0;
        }

        view = new ResolvedBytes(arr, off);
        resolvedBytes = view;
        return view;
    }

    protected final void checkIndex(int i) {
        if (i < 0 || i >= dimension) {
            throw new IndexOutOfBoundsException("i=" + i + " dim=" + dimension);
        }
    }

    protected static int decodeIntLEAt(final byte[] a, final int p) {
        return (a[p] & 0xFF) | ((a[p + 1] & 0xFF) << 8) | ((a[p + 2] & 0xFF) << 16) | ((a[p + 3] & 0xFF) << 24);
    }

    protected static long decodeLongLEAt(final byte[] a, final int p) {
        // Decode bytes directly; mask each signed byte as unsigned before shifting.
        return ((long) a[p] & 0xFFL) | (((long) a[p + 1] & 0xFFL) << 8) | (((long) a[p + 2] & 0xFFL) << 16) | (((long) a[p + 3] & 0xFFL)
            << 24) | (((long) a[p + 4] & 0xFFL) << 32) | (((long) a[p + 5] & 0xFFL) << 40) | (((long) a[p + 6] & 0xFFL) << 48)
            | (((long) a[p + 7] & 0xFFL) << 56);
    }

    protected static float decodeFloatLEAt(final byte[] a, final int p) {
        return Float.intBitsToFloat(decodeIntLEAt(a, p));
    }

    protected static double decodeDoubleLEAt(final byte[] a, final int p) {
        return Double.longBitsToDouble(decodeLongLEAt(a, p));
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

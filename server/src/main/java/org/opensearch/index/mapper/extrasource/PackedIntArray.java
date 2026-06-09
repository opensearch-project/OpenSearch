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
 * Packed little-endian ints.
 *
 * <p>Canonical storage is a {@link BytesReference} for efficient transport.</p>
 */
final class PackedIntArray extends AbstractPackedArray implements IntArrayValue {

    private volatile int[] cached;

    static PackedIntArray fromPackedBytes(BytesReference packed, int dimension) {
        Objects.requireNonNull(packed, "packed must not be null");
        return new PackedIntArray(packed, dimension, null, 0);
    }

    static PackedIntArray fromPackedArray(byte[] packedArray, int dimension) {
        Objects.requireNonNull(packedArray, "packedArray must not be null");
        return fromPackedArray(packedArray, 0, packedArray.length, dimension);
    }

    static PackedIntArray fromPackedArray(byte[] packedArray, int offset, int length, int dimension) {
        Objects.requireNonNull(packedArray, "packedArray must not be null");
        BytesReference packed = new BytesArray(packedArray, offset, length);
        return new PackedIntArray(packed, dimension, packedArray, offset);
    }

    private PackedIntArray(BytesReference packed, int dimension, byte[] bytes, int bytesOffset) {
        super(packed, dimension, bytes, bytesOffset, Integer.BYTES, "int");
    }

    static PackedIntArray readBodyFrom(StreamInput in, int dim) throws IOException {
        return fromPackedBytes(in.readBytesReference(), dim);
    }

    @Override
    public int get(int i) {
        checkIndex(i);
        int[] v = cached;
        if (v != null) {
            return v[i];
        }
        ResolvedBytes resolved = ensureBytes();
        return decodeIntLEAt(resolved.bytes, resolved.offset + i * Integer.BYTES);
    }

    @Override
    public int[] asIntArray() {
        int[] v = cached;
        if (v != null) return v;

        ResolvedBytes resolved = ensureBytes();

        v = new int[dimension];
        int p = resolved.offset;
        for (int i = 0; i < dimension; i++) {
            v[i] = decodeIntLEAt(resolved.bytes, p);
            p += Integer.BYTES;
        }

        cached = v;
        return v;
    }
}

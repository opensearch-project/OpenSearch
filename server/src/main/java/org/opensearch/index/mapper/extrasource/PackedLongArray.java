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
 * Packed little-endian longs.
 *
 * <p>Canonical storage is a {@link BytesReference} for efficient transport.</p>
 */
final class PackedLongArray extends AbstractPackedArray implements LongArrayValue {

    private volatile long[] cached;

    static PackedLongArray fromPackedBytes(BytesReference packed, int dimension) {
        Objects.requireNonNull(packed, "packed must not be null");
        return new PackedLongArray(packed, dimension, null, 0);
    }

    static PackedLongArray fromPackedArray(byte[] packedArray, int dimension) {
        Objects.requireNonNull(packedArray, "packedArray must not be null");
        return fromPackedArray(packedArray, 0, packedArray.length, dimension);
    }

    static PackedLongArray fromPackedArray(byte[] packedArray, int offset, int length, int dimension) {
        Objects.requireNonNull(packedArray, "packedArray must not be null");
        BytesReference packed = new BytesArray(packedArray, offset, length);
        return new PackedLongArray(packed, dimension, packedArray, offset);
    }

    private PackedLongArray(BytesReference packed, int dimension, byte[] bytes, int bytesOffset) {
        super(packed, dimension, bytes, bytesOffset, Long.BYTES, "long");
    }

    static PackedLongArray readBodyFrom(StreamInput in, int dim) throws IOException {
        return fromPackedBytes(in.readBytesReference(), dim);
    }

    @Override
    public long get(int i) {
        checkIndex(i);
        long[] v = cached;
        if (v != null) {
            return v[i];
        }
        ResolvedBytes resolved = ensureBytes();
        return decodeLongLEAt(resolved.bytes, resolved.offset + i * Long.BYTES);
    }

    @Override
    public long[] asLongArray() {
        long[] v = cached;
        if (v != null) return v;

        ResolvedBytes resolved = ensureBytes();

        v = new long[dimension];
        int p = resolved.offset;
        for (int i = 0; i < dimension; i++) {
            v[i] = decodeLongLEAt(resolved.bytes, p);
            p += Long.BYTES;
        }

        cached = v;
        return v;
    }
}

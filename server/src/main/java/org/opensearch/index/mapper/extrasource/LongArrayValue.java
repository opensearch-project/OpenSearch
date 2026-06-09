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
 * A long array value used in {@link ExtraFieldValue}.
 *
 * <p>Supports both primitive (decoded) and packed (little-endian) representations.</p>
 */
public non-sealed interface LongArrayValue extends ExtraFieldValue {

    @Override
    default Type type() {
        return Type.LONG_ARRAY;
    }

    /**
     * Creates a {@link LongArrayValue} from packed little-endian bytes.
     *
     * @param packed the packed long bytes (dimension * 8 bytes)
     * @param dimension the number of long elements
     * @return a long-array value backed by the provided bytes
     * @throws IllegalArgumentException if the byte length does not match {@code dimension * 8}
     */
    static LongArrayValue fromPackedBytes(BytesReference packed, int dimension) {
        return PackedLongArray.fromPackedBytes(packed, dimension);
    }

    /**
     * Creates a {@link LongArrayValue} from a packed little-endian byte array.
     *
     * @param packed the packed long bytes (dimension * 8 bytes)
     * @param dimension the number of long elements
     * @return a long-array value backed by the provided array
     * @throws IllegalArgumentException if the byte length does not match {@code dimension * 8}
     */
    static LongArrayValue fromPackedArray(byte[] packed, int dimension) {
        return PackedLongArray.fromPackedArray(packed, dimension);
    }

    /**
     * Creates a {@link LongArrayValue} from a long array.
     *
     * @param values the long values
     * @return a long-array value backed by the provided array
     */
    static LongArrayValue fromLongArray(long[] values) {
        return new PrimitiveLongArray(values);
    }

    /** Number of long elements. */
    int dimension();

    /** True if backed by little-endian packed bytes. */
    boolean isPackedLE();

    /**
     * Packed bytes for the long vector (LE, 8 * dimension bytes).
     * Only valid when isPackedLE() == true.
     */
    BytesReference packedBytes();

    /** Random access (packed reads 8 bytes at i*8; non-packed returns v[i]). */
    long get(int i);

    /**
     * Convenience; allocates a long array for packed values.
     * Zero-copy decoding is used when the packed value is backed by a usable byte array.
     * For other BytesReference implementations, decoding may lazily materialize one cached
     * byte array.
     */
    long[] asLongArray();

    @Override
    default int size() {
        return dimension();
    }

    @Override
    default void writeBodyTo(StreamOutput out) throws IOException {
        out.writeBoolean(isPackedLE());
        writePayloadTo(out);
    }

    void writePayloadTo(StreamOutput out) throws IOException;

    static LongArrayValue readBodyFrom(StreamInput in) throws IOException {
        final boolean packedLE = in.readBoolean();
        if (packedLE) {
            final int dim = in.readVInt();
            return PackedLongArray.readBodyFrom(in, dim);
        } else {
            return PrimitiveLongArray.readBodyFrom(in);
        }
    }
}

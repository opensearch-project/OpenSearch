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
 * An int array value used in {@link ExtraFieldValue}.
 *
 * <p>Supports both primitive (decoded) and packed (little-endian) representations.</p>
 */
public non-sealed interface IntArrayValue extends ExtraFieldValue {

    @Override
    default Type type() {
        return Type.INT_ARRAY;
    }

    /**
     * Creates an {@link IntArrayValue} from packed little-endian bytes.
     *
     * @param packed the packed int bytes (dimension * 4 bytes)
     * @param dimension the number of int elements
     * @return an int-array value backed by the provided bytes
     * @throws IllegalArgumentException if the byte length does not match {@code dimension * 4}
     */
    static IntArrayValue fromPackedBytes(BytesReference packed, int dimension) {
        return PackedIntArray.fromPackedBytes(packed, dimension);
    }

    /**
     * Creates an {@link IntArrayValue} from a packed little-endian byte array.
     *
     * @param packed the packed int bytes (dimension * 4 bytes)
     * @param dimension the number of int elements
     * @return an int-array value backed by the provided array
     * @throws IllegalArgumentException if the byte length does not match {@code dimension * 4}
     */
    static IntArrayValue fromPackedArray(byte[] packed, int dimension) {
        return PackedIntArray.fromPackedArray(packed, dimension);
    }

    /**
     * Creates an {@link IntArrayValue} from an int array.
     *
     * @param values the int values
     * @return an int-array value backed by the provided array
     */
    static IntArrayValue fromIntArray(int[] values) {
        return new PrimitiveIntArray(values);
    }

    /** Number of int elements. */
    int dimension();

    /** True if backed by little-endian packed bytes. */
    boolean isPackedLE();

    /**
     * Packed bytes for the int vector (LE, 4 * dimension bytes).
     * Only valid when isPackedLE() == true.
     */
    BytesReference packedBytes();

    /** Random access (packed reads 4 bytes at i*4; non-packed returns v[i]). */
    int get(int i);

    /**
     * Convenience; allocates an int array for packed values.
     * Zero-copy decoding is used when the packed value is backed by a usable byte array.
     * For other BytesReference implementations, decoding may lazily materialize one cached
     * byte array.
     */
    int[] asIntArray();

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

    static IntArrayValue readBodyFrom(StreamInput in) throws IOException {
        final boolean packedLE = in.readBoolean();
        if (packedLE) {
            final int dim = in.readVInt();
            return PackedIntArray.readBodyFrom(in, dim);
        } else {
            return PrimitiveIntArray.readBodyFrom(in);
        }
    }
}

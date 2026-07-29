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
 * A double array value used in {@link ExtraFieldValue}.
 *
 * <p>Supports both primitive (decoded) and packed (little-endian) representations.</p>
 */
public non-sealed interface DoubleArrayValue extends ExtraFieldValue {

    @Override
    default Type type() {
        return Type.DOUBLE_ARRAY;
    }

    /**
     * Creates a {@link DoubleArrayValue} from packed little-endian bytes.
     *
     * @param packed the packed double bytes (dimension * 8 bytes)
     * @param dimension the number of double elements
     * @return a double-array value backed by the provided bytes
     * @throws IllegalArgumentException if the byte length does not match {@code dimension * 8}
     */
    static DoubleArrayValue fromPackedBytes(BytesReference packed, int dimension) {
        return PackedDoubleArray.fromPackedBytes(packed, dimension);
    }

    /**
     * Creates a {@link DoubleArrayValue} from a packed little-endian byte array.
     *
     * @param packed the packed double bytes (dimension * 8 bytes)
     * @param dimension the number of double elements
     * @return a double-array value backed by the provided array
     * @throws IllegalArgumentException if the byte length does not match {@code dimension * 8}
     */
    static DoubleArrayValue fromPackedArray(byte[] packed, int dimension) {
        return PackedDoubleArray.fromPackedArray(packed, dimension);
    }

    /**
     * Creates a {@link DoubleArrayValue} from a double array.
     *
     * @param values the double values
     * @return a double-array value backed by the provided array
     */
    static DoubleArrayValue fromDoubleArray(double[] values) {
        return new PrimitiveDoubleArray(values);
    }

    /** Number of double elements. */
    int dimension();

    /** True if backed by little-endian packed bytes. */
    boolean isPackedLE();

    /**
     * Packed bytes for the double vector (LE, 8 * dimension bytes).
     * Only valid when isPackedLE() == true.
     */
    BytesReference packedBytes();

    /** Random access (packed reads 8 bytes at i*8; non-packed returns v[i]). */
    double get(int i);

    /**
     * Convenience; allocates a double array for packed values.
     * Zero-copy decoding is used when the packed value is backed by a usable byte array.
     * For other BytesReference implementations, decoding may lazily materialize one cached
     * byte array.
     */
    double[] asDoubleArray();

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

    static DoubleArrayValue readBodyFrom(StreamInput in) throws IOException {
        final boolean packedLE = in.readBoolean();
        if (packedLE) {
            final int dim = in.readVInt();
            return PackedDoubleArray.readBodyFrom(in, dim);
        } else {
            return PrimitiveDoubleArray.readBodyFrom(in);
        }
    }
}

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
 * A float array value used in {@link ExtraFieldValue}.
 *
 * <p>Supports both primitive (decoded) and packed (little-endian) representations.</p>
 */
public non-sealed interface FloatArrayValue extends ExtraFieldValue {

    @Override
    default Type type() {
        return Type.FLOAT_ARRAY;
    }

    /**
     * Creates a {@link FloatArrayValue} from packed little-endian bytes.
     *
     * @param packed the packed float bytes (dimension * 4 bytes)
     * @param dimension the number of float elements
     * @return a float-array value backed by the provided bytes
     * @throws IllegalArgumentException if the byte length does not match {@code dimension * 4}
     */
    static FloatArrayValue fromPackedBytes(BytesReference packed, int dimension) {
        return PackedFloatArray.fromPackedBytes(packed, dimension);
    }

    /**
     * Creates a {@link FloatArrayValue} from a packed little-endian byte array.
     *
     * @param packed the packed float bytes (dimension * 4 bytes)
     * @param dimension the number of float elements
     * @return a float-array value backed by the provided array
     * @throws IllegalArgumentException if the byte length does not match {@code dimension * 4}
     */
    static FloatArrayValue fromPackedArray(byte[] packed, int dimension) {
        return PackedFloatArray.fromPackedArray(packed, dimension);
    }

    /**
     * Creates a {@link FloatArrayValue} from a float array.
     *
     * @param values the float values
     * @return a float-array value backed by the provided array
     */
    static FloatArrayValue fromFloatArray(float[] values) {
        return new PrimitiveFloatArray(values);
    }

    /** Number of float elements. */
    int dimension();

    /** True if backed by little-endian packed bytes. */
    boolean isPackedLE();

    /**
     * Packed bytes for the float vector (LE, 4 * dimension bytes).
     * Only valid when isPackedLE() == true.
     */
    BytesReference packedBytes();

    /** Random access (packed reads 4 bytes at i*4; non-packed returns v[i]). */
    float get(int i);

    /**
     * Convenience; may allocate/copy for packed (allocating float[] is unavoidable).
     * Packed implementation must NOT perform an extra byte[] compaction copy.
     */
    float[] asFloatArray();

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

    static FloatArrayValue readBodyFrom(StreamInput in) throws IOException {
        final boolean packedLE = in.readBoolean();
        if (packedLE) {
            final int dim = in.readVInt();
            return PackedFloatArray.readBodyFrom(in, dim);
        } else {
            return PrimitiveFloatArray.readBodyFrom(in);
        }
    }
}

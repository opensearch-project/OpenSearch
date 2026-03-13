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

public non-sealed interface FloatArrayValue extends ExtraFieldValue {

    @Override
    default Type type() {
        return Type.FLOAT_ARRAY;
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;

/**
 * Sizes record batches as Arrow IPC messages, so a batch can be cut to fit its byte budget before it is sent.
 */
final class BatchBytes {

    /** Upper bound of the encapsulation and flatbuffer metadata of a record batch message, without its per-column parts. */
    static final long MESSAGE_OVERHEAD = 128;
    /** Upper bound of the metadata of one vector of a column: its field node and up to three buffer descriptors. */
    static final long COLUMN_OVERHEAD = 64;

    private BatchBytes() {}

    /**
     * Returns the IPC message size of the first {@code rows} rows of a batch.
     *
     * @param root a batch whose vectors hold at least {@code rows} values
     */
    static long messageBytes(VectorSchemaRoot root, int rows) {
        long bytes = metadataBytes(root);
        for (FieldVector vector : root.getFieldVectors()) {
            bytes += bodyBytes(vector, rows);
        }
        return bytes;
    }

    /** Returns an upper bound of the metadata of a record batch message of {@code root}, whatever its row count. */
    static long metadataBytes(VectorSchemaRoot root) {
        long bytes = MESSAGE_OVERHEAD;
        for (FieldVector vector : root.getFieldVectors()) {
            bytes += metadataBytes(vector);
        }
        return bytes;
    }

    private static long metadataBytes(FieldVector vector) {
        long bytes = COLUMN_OVERHEAD;
        for (FieldVector child : vector.getChildrenFromFields()) {
            bytes += metadataBytes(child);
        }
        return bytes;
    }

    /**
     * Returns the size of a vector's buffers for its first {@code rows} rows, each buffer padded to 8 bytes as IPC lays it
     * out.
     *
     * @param vector a vector of a column type, or a list of one, holding at least {@code rows} values
     */
    static long bodyBytes(FieldVector vector, int rows) {
        final long validity = pad(bits(rows));
        if (vector instanceof BitVector) {
            return validity + pad(bits(rows));
        }
        if (vector instanceof VarCharVector varChar) {
            final long data = rows == 0 ? 0 : varChar.getOffsetBuffer().getInt((long) rows * VarCharVector.OFFSET_WIDTH);
            return validity + pad((rows + 1L) * VarCharVector.OFFSET_WIDTH) + pad(data);
        }
        if (vector instanceof ListVector list) {
            final int items = rows == 0 ? 0 : list.getOffsetBuffer().getInt((long) rows * ListVector.OFFSET_WIDTH);
            return validity + pad((rows + 1L) * ListVector.OFFSET_WIDTH) + bodyBytes(list.getDataVector(), items);
        }
        if (vector instanceof BaseFixedWidthVector fixed) {
            return validity + pad((long) rows * fixed.getTypeWidth());
        }
        throw new IllegalStateException("unexpected vector type " + vector.getClass().getName());
    }

    private static long bits(int rows) {
        return (rows + 7L) >>> 3;
    }

    private static long pad(long bytes) {
        return (bytes + 7) & ~7L;
    }
}

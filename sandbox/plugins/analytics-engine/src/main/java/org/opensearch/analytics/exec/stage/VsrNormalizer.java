/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Normalizes Arrow {@link VectorSchemaRoot}s received from data-node shards so that
 * variable-width string columns are materialized as {@link VarCharVector} (Utf8)
 * regardless of what the shard produced ({@link ViewVarCharVector} / {@link LargeVarCharVector}).
 *
 * <p>DataFusion's physical plan can promote {@code Utf8} to {@code Utf8View} inside
 * aggregations (group-by keys, hash tables). When the shard ships that VSR to the
 * coordinator via Arrow C Data + Flight, the coordinator's reduce-sink partition
 * stream is declared as {@code Utf8} (the type Calcite emits). The buffer layout of
 * {@code Utf8View} (16-byte per-entry views) doesn't round-trip through a {@code Utf8}
 * declaration — the consumer reinterprets view bytes as 32-bit offsets, producing
 * garbage offsets that either OOM the heap ({@code new byte[endOffset - startOffset]}
 * with a multi-GB length) or yield {@link NegativeArraySizeException}.
 *
 * <p>This normalizer copies each view/large-varchar column into a fresh
 * {@link VarCharVector}, swaps it into the VSR, and closes the original. Other vector
 * types pass through untouched.
 */
final class VsrNormalizer {

    private VsrNormalizer() {}

    /**
     * Returns a VSR whose variable-width string columns are all {@code Utf8}. If no
     * columns need conversion, returns {@code vsr} unchanged. Otherwise returns a new
     * VSR built from {@code vsr}'s contents — caller must close whichever is returned
     * (the returned VSR owns every field vector, including any that were passed through).
     */
    static VectorSchemaRoot toUtf8Strings(VectorSchemaRoot vsr, BufferAllocator allocator) {
        if (!hasNonUtf8StringColumn(vsr)) {
            return vsr;
        }

        int rowCount = vsr.getRowCount();
        List<FieldVector> newVectors = new ArrayList<>(vsr.getFieldVectors().size());
        List<Field> newFields = new ArrayList<>(vsr.getFieldVectors().size());

        try {
            for (FieldVector original : vsr.getFieldVectors()) {
                FieldVector replacement;
                if (original instanceof ViewVarCharVector view) {
                    replacement = copyToVarChar(view.getName(), rowCount, allocator, view::isNull, view::get);
                } else if (original instanceof LargeVarCharVector large) {
                    replacement = copyToVarChar(large.getName(), rowCount, allocator, large::isNull, large::get);
                } else {
                    // Transfer ownership so the original VSR's close() won't free the buffers.
                    replacement = transferVector(original, allocator);
                }
                newVectors.add(replacement);
                newFields.add(replacement.getField());
            }
        } catch (RuntimeException e) {
            for (FieldVector v : newVectors)
                v.close();
            throw e;
        }

        VectorSchemaRoot normalized = new VectorSchemaRoot(new Schema(newFields), newVectors, rowCount);
        vsr.close();
        return normalized;
    }

    private static boolean hasNonUtf8StringColumn(VectorSchemaRoot vsr) {
        for (FieldVector v : vsr.getFieldVectors()) {
            if (v instanceof ViewVarCharVector || v instanceof LargeVarCharVector) {
                return true;
            }
        }
        return false;
    }

    private static VarCharVector copyToVarChar(
        String name,
        int rowCount,
        BufferAllocator allocator,
        java.util.function.IntPredicate isNull,
        java.util.function.IntFunction<byte[]> getBytes
    ) {
        Field field = new Field(name, FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        VarCharVector dst = (VarCharVector) field.createVector(allocator);
        try {
            dst.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (isNull.test(i)) {
                    dst.setNull(i);
                } else {
                    dst.setSafe(i, getBytes.apply(i));
                }
            }
            dst.setValueCount(rowCount);
            return dst;
        } catch (RuntimeException e) {
            dst.close();
            throw e;
        }
    }

    private static FieldVector transferVector(FieldVector src, BufferAllocator allocator) {
        FieldVector dst = src.getField().createVector(allocator);
        src.makeTransferPair(dst).transfer();
        return dst;
    }
}

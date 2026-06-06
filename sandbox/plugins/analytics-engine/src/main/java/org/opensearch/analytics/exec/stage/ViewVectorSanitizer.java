/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseVariableWidthViewVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Zeroes the view of every null slot in a hand-built batch's view columns (Utf8View / BinaryView).
 *
 * <p>When a producer fills a view vector with {@code copyFromSafe}/{@code setNull}, Arrow unsets
 * the validity bit but leaves the slot's 16-byte view as uninitialized buffer memory. Arrow-Java
 * honors the validity bit, but once the batch is exported across the Arrow C Data Interface,
 * DataFusion's interleave reads the view length prefix unconditionally — a garbage length
 * dereferences a data buffer that does not exist on an all-null column and panics. Zeroing makes
 * each null slot a valid length-0 inline view; validity bits are untouched, so cells stay null.
 *
 * <p>Called by the producer that builds the batch (the {@link Stitcher}); batches imported from
 * native are already spec-valid and never need this. Mutates in place — no copy.
 */
final class ViewVectorSanitizer {

    private ViewVectorSanitizer() {}

    static void sanitize(VectorSchemaRoot root) {
        int rows = root.getRowCount();
        for (FieldVector vec : root.getFieldVectors()) {
            if (!(vec instanceof BaseVariableWidthViewVector view) || view.getNullCount() == 0) continue;
            ArrowBuf viewBuffer = view.getDataBuffer();
            for (int row = 0; row < rows; row++) {
                if (view.isNull(row)) {
                    viewBuffer.setZero((long) row * BaseVariableWidthViewVector.ELEMENT_SIZE, BaseVariableWidthViewVector.ELEMENT_SIZE);
                }
            }
        }
    }
}

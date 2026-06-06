/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseVariableWidthViewVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Zeroes the view of every null slot in a batch's view columns (Utf8View / BinaryView) before it
 * crosses the Arrow C Data Interface into native code.
 *
 * <p>Arrow's null-write path unsets the validity bit but leaves the slot's 16-byte view as
 * uninitialized buffer memory. Arrow-Java honors the validity bit, but DataFusion's interleave
 * reads the view length prefix unconditionally and a garbage length dereferences a data buffer
 * that does not exist on an all-null column, panicking. Zeroing makes each null slot a valid
 * length-0 inline view; validity bits are untouched. Mutates in place, so export stays zero-copy.
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

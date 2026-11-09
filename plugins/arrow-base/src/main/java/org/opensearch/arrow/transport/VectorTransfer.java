/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.transport;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.TransferPair;

import java.util.List;

/**
 * Zero-copy helpers for moving {@link VectorSchemaRoot} contents between allocators.
 *
 * @opensearch.internal
 */
public final class VectorTransfer {

    private VectorTransfer() {}

    /**
     * Zero-copy transfers every vector from {@code source} into {@code target}. After this call,
     * the target owns the buffers and holds the row count; the source is empty with row count 0.
     *
     * @param source the root whose vectors are being moved out
     * @param target the root receiving the vectors
     */
    public static void transferRoot(VectorSchemaRoot source, VectorSchemaRoot target) {
        List<FieldVector> sources = source.getFieldVectors();
        List<FieldVector> targets = target.getFieldVectors();
        for (int i = 0; i < sources.size(); i++) {
            TransferPair tp = sources.get(i).makeTransferPair(targets.get(i));
            tp.transfer();
        }
        target.setRowCount(source.getRowCount());
        source.setRowCount(0);
    }
}

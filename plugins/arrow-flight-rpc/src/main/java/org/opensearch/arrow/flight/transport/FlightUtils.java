/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.TransferPair;

import java.util.List;

class FlightUtils {

    private FlightUtils() {}

    static long calculateVectorSchemaRootSize(VectorSchemaRoot root) {
        if (root == null) {
            return 0;
        }
        long totalSize = 0;
        for (int i = 0; i < root.getFieldVectors().size(); i++) {
            var vector = root.getVector(i);
            if (vector != null) {
                totalSize += vector.getBufferSize();
            }
        }
        return totalSize;
    }

    /**
     * Zero-copy transfers every vector from {@code source} into {@code target} and copies
     * the row count. After this call, the source vectors are empty and the target owns
     * the buffers.
     */
    static void transferRoot(VectorSchemaRoot source, VectorSchemaRoot target) {
        List<FieldVector> sources = source.getFieldVectors();
        List<FieldVector> targets = target.getFieldVectors();
        for (int i = 0; i < sources.size(); i++) {
            TransferPair tp = sources.get(i).makeTransferPair(targets.get(i));
            tp.transfer();
        }
        target.setRowCount(source.getRowCount());
    }
}

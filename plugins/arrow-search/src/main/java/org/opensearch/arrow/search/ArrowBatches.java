/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;

/**
 * Operations on the Arrow batches of a search.
 */
public final class ArrowBatches {

    private ArrowBatches() {}

    /**
     * Sets the row count of a batch and drops the rows written past it, including the values of dropped list rows.
     *
     * @param root the batch
     * @param rows the new row count
     */
    public static void setRowCount(VectorSchemaRoot root, int rows) {
        for (FieldVector vector : root.getFieldVectors()) {
            if (vector instanceof ListVector list) {
                // A list vector sizes its values from its last-set row, so that index must first move back to the last kept row.
                list.setLastSet(rows - 1);
            }
        }
        root.setRowCount(rows);
    }
}

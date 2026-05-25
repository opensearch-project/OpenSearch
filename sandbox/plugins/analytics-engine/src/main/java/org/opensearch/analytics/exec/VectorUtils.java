/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for Arrow {@link VectorSchemaRoot} manipulation in the analytics engine.
 *
 * @opensearch.internal
 */
public final class VectorUtils {

    private static final Logger LOGGER = LogManager.getLogger(VectorUtils.class);

    private VectorUtils() {}

    /**
     * Builds a new {@link VectorSchemaRoot} whose schema is the input's schema with
     * an additional non-nullable Int32 column appended at the end. The returned VSR's
     * existing columns are the same {@link org.apache.arrow.vector.FieldVector}
     * instances as the input — only the new constant column is a fresh allocation.
     * No data is copied.
     *
     * <p>Position contract: the new column is appended at the end of the schema. This
     * must align with the planner-side declaration that adds helper columns (e.g.
     * {@code ___ugsi}) at the end of the row type — see
     * {@code RelNodeUtils.appendField}.
     *
     * <p>TODO: position is hard-coded to "end" today because the planner happens to
     * declare helper columns at the end. If a future helper is declared in the middle
     * of the row type, this helper needs to take a column index parameter (or a name
     * + reference schema to look up the index) instead of assuming the tail. Coupling
     * the runtime append position to the planner's declared position is the design
     * goal; we just don't pay the index-lookup cost today.
     *
     * <p>Ownership semantics: the returned VSR shares its leading FieldVectors with
     * the input by reference (Arrow ref-counting). Callers must not close the input
     * VSR independently — closing the returned VSR releases both the new column and
     * the shared columns.
     *
     * <p>Implementation: builds a new VSR from the existing FieldVector references plus the
     * constant column. We avoid {@code VectorSchemaRoot.addVector} because Arrow 18.x's
     * precondition rejects appending at index == size (only insert before an existing column
     * is allowed); the Iterable constructor accepts the appended layout directly.
     *
     * @param input     source batch; not closed by this method
     * @param name      name of the new column
     * @param value     constant value written to every row of the new column
     * @param allocator allocator for the new {@link IntVector}
     * @return new VSR with the appended column; row count and existing column data unchanged
     */
    public static VectorSchemaRoot appendConstantInt(VectorSchemaRoot input, String name, int value, BufferAllocator allocator) {
        int rowCount = input.getRowCount();
        // FIXME [RemoveBeforeMainMerge] diagnostics for QTF VSR.addVector failure
        LOGGER.info(
            "FIXME [RemoveBeforeMainMerge] appendConstantInt: name={} value={} rowCount={} fieldVectorsSize={} schema={}",
            name,
            value,
            rowCount,
            input.getFieldVectors().size(),
            input.getSchema()
        );

        // TODO/FIXME: reserve `rowCount * 4` bytes against the query CircuitBreaker before allocateNew.
        IntVector constantVector = new IntVector(name, allocator);
        constantVector.allocateNew(rowCount);
        for (int i = 0; i < rowCount; i++) {
            constantVector.set(i, value);
        }
        constantVector.setValueCount(rowCount);

        // Zero-copy: existing FieldVectors are reused by reference; only the new column allocates.
        List<FieldVector> combined = new ArrayList<>(input.getFieldVectors().size() + 1);
        combined.addAll(input.getFieldVectors());
        combined.add(constantVector);
        return new VectorSchemaRoot(combined);
    }
}

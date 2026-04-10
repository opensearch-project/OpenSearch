/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import java.util.List;

/**
 * Read-only view of a single record batch. Provides field names, row count,
 * and positional access to field values.
 * <p>
 * A batch is only valid until the next call to {@link java.util.Iterator#next()}
 * on the parent stream's iterator. The underlying data buffers may be reused
 * across batches, so callers must extract all needed values before advancing
 * the iterator. Accessing a batch after the iterator has advanced may throw
 * {@link IllegalStateException}.
 *
 * @opensearch.internal
 */
public interface EngineResultBatch {

    /**
     * Ordered list of field (column) names in this batch.
     */
    List<String> getFieldNames();

    /**
     * Number of rows in this batch.
     */
    int getRowCount();

    /**
     * Returns the value at the given row index for the named field.
     *
     * @param fieldName column name
     * @param rowIndex  zero-based row index
     * @return the value (may be null)
     */
    Object getFieldValue(String fieldName, int rowIndex);
}

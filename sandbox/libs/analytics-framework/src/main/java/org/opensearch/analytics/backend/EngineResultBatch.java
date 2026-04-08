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
 *
 * @opensearch.internal
 */
public interface EngineResultBatch {

    /**
     * Ordered list of field (column) names in this batch.
     *
     * @return field names
     */
    List<String> getFieldNames();

    /**
     * Number of rows in this batch.
     *
     * @return row count
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

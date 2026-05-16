/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Represents a document input for adding fields and metadata to a writer.
 *
 * @param <T> the type of the final input representation
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DocumentInput<T> extends AutoCloseable {

    /** Standard field name for the row ID used to correlate documents across data formats. */
    String ROW_ID_FIELD = "__row_id__";

    /**
     * Gets the final input representation.
     *
     * @return the final input of type T
     */
    T getFinalInput();

    /**
     * Adds a field to the document.
     *
     * @param fieldType the mapped field type
     * @param value the field value
     */
    void addField(MappedFieldType fieldType, Object value);

    /**
     * Adds a row ID field to the document.
     *
     * @param rowIdFieldName the name of the row ID field
     * @param rowId the row ID value
     */
    void setRowId(String rowIdFieldName, long rowId);

    /**
     * Given a field name, returns the number of values associated with that field in the document.
     * @param fieldName name of the field to lookup
     * @return count of field values
     */
    long getFieldCount(String fieldName);
}

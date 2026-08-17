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

import java.util.List;

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

    /**
     * Offers one element of a {@code nested} field's array to the format. Called once per element,
     * in source order, while the enclosing document is being parsed. The two lists are parallel:
     * {@code values.get(i)} is the raw source value for {@code fieldTypes.get(i)}.
     * <p>
     * Formats that store nested elements as rows of a separate child table (see the child-table
     * design) override this to stage the element; the enclosing document's row id becomes the
     * element's foreign key once the row id is known. The default is a no-op, so a format that
     * does not model nested elements simply ignores them.
     *
     * @param nestedPath the full path of the nested object mapper (e.g. {@code user})
     * @param ordinal    the element's 0-based position in the source array
     * @param fieldTypes the mapped field types of the element's leaf fields
     * @param values     the raw source values, parallel to {@code fieldTypes}
     */
    default void addNestedElement(String nestedPath, int ordinal, List<MappedFieldType> fieldTypes, List<Object> values) {
        // no-op by default: formats that do not model nested elements ignore them
    }
}

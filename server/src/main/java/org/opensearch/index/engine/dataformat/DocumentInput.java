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
     * Signals that a new element of an identity-bearing sub-scope begins — a mapping construct
     * whose array elements are distinct logical sub-documents with per-element field correlation.
     * The {@code nested} field type is the only such construct today; a future type with the same
     * per-element identity semantics (e.g. a variant type) uses these same signals.
     *
     * <p>Document parsing emits this once per array element (and once for a single object value),
     * before any of the element's fields arrive through {@link #addField}. Elements declared
     * inside another element's scope produce properly nested start/end pairs. Every call is
     * matched by exactly one {@link #endNestedElement()}, even when parsing fails midway through
     * the element.
     *
     * <p>Plain {@code object} fields do not emit these signals: an object is a namespace, fully
     * identified by the dotted field names arriving through {@link #addField}. Formats without a
     * per-element representation can ignore both signals; the default implementations are no-ops.
     *
     * @param path the full dotted path of the field this element belongs to
     */
    default void startNestedElement(String path) {}

    /**
     * Signals that the element opened by the matching {@link #startNestedElement(String)} ends.
     * Fields arriving after this call belong to the enclosing scope. The default implementation
     * is a no-op.
     */
    default void endNestedElement() {}

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

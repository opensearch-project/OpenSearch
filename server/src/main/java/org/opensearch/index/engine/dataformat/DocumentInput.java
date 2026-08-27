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
     * Signals the start of a nested child object at {@code nestedPath} (the full dotted mapper path).
     * Subsequent {@link #addField} calls, until the matching {@link #endNestedChild()}, belong to this
     * nested child (or a deeper one). A format that flattens nested arrays into columnar structures
     * (e.g. Parquet) begins a new element here; a format that has no notion of nested structure at all
     * can simply skip fields received in this scope. Nesting composes to arbitrary depth.
     *
     * <p>Default is a no-op so formats with no nested handling are unaffected and existing callers that
     * never emit these signals keep their current behavior.
     *
     * @param nestedPath the full dotted path of the nested object (e.g. {@code comments.replies})
     */
    default void startNestedChild(String nestedPath) {}

    /**
     * Signals the end of the innermost open nested child (the match to the most recent
     * {@link #startNestedChild(String)}). Default is a no-op.
     */
    default void endNestedChild() {}

    /**
     * Emits one {@code (key, value)} entry of a map-typed field (e.g. a {@code flat_object}'s open key
     * space). Called once per leaf, in document parse order, instead of {@link #addField} — a columnar
     * format backs such a field with a single {@code MAP<key, value>} column, so the open key set is
     * stored losslessly against a static schema.
     *
     * <p>When emitted between {@link #startNestedChild(String)} and {@link #endNestedChild()} the entry
     * belongs to that nested element's map child; otherwise it belongs to a document-root map column.
     *
     * <p>Default is a no-op so formats with no map notion (e.g. Lucene, which stores flat_object as its
     * own {@code _value} / {@code _valueAndPath} terms) are unaffected.
     *
     * @param mapField the map-typed field the entry belongs to
     * @param key the entry key — the leaf's dotted path relative to {@code mapField}
     * @param value the entry value, or {@code null}
     */
    default void addMapEntry(MappedFieldType mapField, String key, Object value) {}

    /**
     * Given a field name, returns the number of values associated with that field in the document.
     * @param fieldName name of the field to lookup
     * @return count of field values
     */
    long getFieldCount(String fieldName);
}

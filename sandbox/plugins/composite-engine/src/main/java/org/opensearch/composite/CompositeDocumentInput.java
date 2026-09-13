/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.MappedFieldType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A composite {@link DocumentInput} that wraps one {@link DocumentInput} per registered data format
 * and broadcasts every operation — metadata and {@link #addField} alike — to all of them unconditionally.
 * <p>
 * There is no nested-scope bookkeeping here: {@code nested} and {@code flat_object} data flow through
 * the same {@link #addField} as everything else. Each per-format {@link DocumentInput} decides for
 * itself whether and how to represent what it's given (see {@code ParquetDocumentInput}), and its own
 * capability self-filter drops anything outside what it was assigned for the field's mapping scope.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeDocumentInput implements DocumentInput<List<? extends DocumentInput<?>>> {

    private final DocumentInput<?> primaryDocumentInput;
    private final DataFormat primaryFormat;
    private final Map<DataFormat, DocumentInput<?>> secondaryDocumentInputs;
    private long rowId = -1L;

    /**
     * Constructs a CompositeDocumentInput with a primary format input and secondary format inputs.
     *
     * @param primaryFormat the primary data format
     * @param primaryDocumentInput the document input for the primary format
     * @param secondaryDocumentInputs a map of secondary data formats to their corresponding document inputs
     */
    public CompositeDocumentInput(
        DataFormat primaryFormat,
        DocumentInput<?> primaryDocumentInput,
        Map<DataFormat, DocumentInput<?>> secondaryDocumentInputs
    ) {
        this.primaryFormat = Objects.requireNonNull(primaryFormat, "primaryFormat must not be null");
        this.primaryDocumentInput = Objects.requireNonNull(primaryDocumentInput, "primaryDocumentInput must not be null");
        this.secondaryDocumentInputs = Collections.unmodifiableMap(
            Objects.requireNonNull(secondaryDocumentInputs, "secondaryDocumentInputs must not be null")
        );
    }

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        addFieldTo(primaryDocumentInput, primaryFormat.name(), fieldType, value);
        for (Map.Entry<DataFormat, DocumentInput<?>> entry : secondaryDocumentInputs.entrySet()) {
            addFieldTo(entry.getValue(), entry.getKey().name(), fieldType, value);
        }
    }

    private static void addFieldTo(DocumentInput<?> input, String formatName, MappedFieldType fieldType, Object value) {
        try {
            input.addField(fieldType, value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to add field [" + fieldType.name() + "] in format [" + formatName + "]", e);
        }
    }

    @Override
    public void setRowId(String rowIdFieldName, long rowId) {
        primaryDocumentInput.setRowId(rowIdFieldName, rowId);
        for (DocumentInput<?> input : secondaryDocumentInputs.values()) {
            input.setRowId(rowIdFieldName, rowId);
        }
        this.rowId = rowId;
    }

    /** Returns the row ID assigned via {@link #setRowId}, or {@code -1} if none. */
    public long getRowId() {
        return rowId;
    }

    public long getFieldCount(String fieldName) {
        // Return the field count from the primary document input
        return primaryDocumentInput.getFieldCount(fieldName);
    }

    @Override
    public List<? extends DocumentInput<?>> getFinalInput() {
        return null;
    }

    @Override
    public void close() {
        // No-op: document input lifecycle is independent of writer pool
    }

    /**
     * Returns the primary format's document input.
     *
     * @return the primary document input
     */
    public DocumentInput<?> getPrimaryInput() {
        return primaryDocumentInput;
    }

    /**
     * Returns the primary data format.
     *
     * @return the primary data format
     */
    public DataFormat getPrimaryFormat() {
        return primaryFormat;
    }

    /**
     * Returns an unmodifiable map of secondary data formats to their document inputs.
     *
     * @return the secondary inputs
     */
    public Map<DataFormat, DocumentInput<?>> getSecondaryInputs() {
        return secondaryDocumentInputs;
    }
}

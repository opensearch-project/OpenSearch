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
import org.opensearch.index.engine.dataformat.NestedAwareDocumentInput;
import org.opensearch.index.mapper.MappedFieldType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A composite {@link DocumentInput} that wraps one {@link DocumentInput} per registered
 * data format and broadcasts all field additions to every per-format input.
 * <p>
 * Metadata operations ({@code setRowId}, {@code setVersion}, {@code setSeqNo},
 * {@code setPrimaryTerm}) and top-level field additions are broadcast to all per-format inputs.
 * <p>
 * Nested-scope signals ({@link #startNestedChild}/{@link #endNestedChild}/{@link #addMapEntry}), and
 * any {@link #addField} call made while inside a nested scope, are forwarded only to per-format inputs
 * that implement {@link NestedAwareDocumentInput} — this class owns the nesting-depth bookkeeping so a
 * format with no nested notion (e.g. Lucene) is never called into for any of it, and needs no bookkeeping
 * of its own.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeDocumentInput implements DocumentInput<List<? extends DocumentInput<?>>> {

    private final DocumentInput<?> primaryDocumentInput;
    private final DataFormat primaryFormat;
    private final Map<DataFormat, DocumentInput<?>> secondaryDocumentInputs;
    private long rowId = -1L;
    private int nestedDepth = 0;

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
        boolean nested = nestedDepth > 0;
        if (nested == false || primaryDocumentInput instanceof NestedAwareDocumentInput) {
            addFieldTo(primaryDocumentInput, primaryFormat.name(), fieldType, value);
        }
        for (Map.Entry<DataFormat, DocumentInput<?>> entry : secondaryDocumentInputs.entrySet()) {
            if (nested == false || entry.getValue() instanceof NestedAwareDocumentInput) {
                addFieldTo(entry.getValue(), entry.getKey().name(), fieldType, value);
            }
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

    @Override
    public void startNestedChild(String nestedPath) {
        // Incremented before the broadcast below (not after) so depth reflects this open even if a
        // per-format call throws partway through — DocumentParser's finally always calls the matching
        // endNestedChild regardless, and that decrement must have a correct increment to pair against.
        nestedDepth++;
        if (primaryDocumentInput instanceof NestedAwareDocumentInput<?> nestedAware) {
            nestedAware.startNestedChild(nestedPath);
        }
        for (DocumentInput<?> input : secondaryDocumentInputs.values()) {
            if (input instanceof NestedAwareDocumentInput<?> nestedAware) {
                nestedAware.startNestedChild(nestedPath);
            }
        }
    }

    @Override
    public void endNestedChild() {
        nestedDepth--;
        if (primaryDocumentInput instanceof NestedAwareDocumentInput<?> nestedAware) {
            nestedAware.endNestedChild();
        }
        for (DocumentInput<?> input : secondaryDocumentInputs.values()) {
            if (input instanceof NestedAwareDocumentInput<?> nestedAware) {
                nestedAware.endNestedChild();
            }
        }
    }

    @Override
    public void addMapEntry(MappedFieldType mapField, String key, Object value) {
        if (primaryDocumentInput instanceof NestedAwareDocumentInput<?> nestedAware) {
            nestedAware.addMapEntry(mapField, key, value);
        }
        for (DocumentInput<?> input : secondaryDocumentInputs.values()) {
            if (input instanceof NestedAwareDocumentInput<?> nestedAware) {
                nestedAware.addMapEntry(mapField, key, value);
            }
        }
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

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
 * A composite {@link DocumentInput} that wraps one {@link DocumentInput} per registered
 * data format and broadcasts all field additions to every per-format input.
 * <p>
 * Metadata operations ({@code setRowId}, {@code setVersion}, {@code setSeqNo},
 * {@code setPrimaryTerm}) and field additions are broadcast to all per-format inputs.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeDocumentInput implements DocumentInput<List<? extends DocumentInput<?>>> {

    private final DocumentInput<?> primaryDocumentInput;
    private final DataFormat primaryFormat;
    private final Map<DataFormat, DocumentInput<?>> secondaryDocumentInputs;
    /**
     * Flat snapshots of {@link #secondaryDocumentInputs}, taken once at construction. {@code addField} and
     * {@code setRowId} run once per field per document on the bulk-indexing hot path; iterating the map there
     * allocates an entry-set iterator and walks the backing table (an {@code IdentityHashMap} in practice —
     * its sparse-table iterator was a visible CPU frame in ingest profiles) on every call. The map is
     * immutable after construction, so the arrays are always in sync with it.
     */
    private final DataFormat[] secondaryFormats;
    private final DocumentInput<?>[] secondaryInputs;
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
        this.secondaryFormats = new DataFormat[secondaryDocumentInputs.size()];
        this.secondaryInputs = new DocumentInput<?>[secondaryDocumentInputs.size()];
        int i = 0;
        for (Map.Entry<DataFormat, DocumentInput<?>> entry : secondaryDocumentInputs.entrySet()) {
            secondaryFormats[i] = entry.getKey();
            secondaryInputs[i] = entry.getValue();
            i++;
        }
    }

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        try {
            primaryDocumentInput.addField(fieldType, value);
        } catch (Exception e) {
            throw new IllegalStateException(
                "Failed to add field [" + fieldType.name() + "] in primary format [" + primaryFormat.name() + "]",
                e
            );
        }
        for (int i = 0; i < secondaryInputs.length; i++) {
            try {
                secondaryInputs[i].addField(fieldType, value);
            } catch (Exception e) {
                throw new IllegalStateException(
                    "Failed to add field [" + fieldType.name() + "] in secondary format [" + secondaryFormats[i].name() + "]",
                    e
                );
            }
        }
    }

    @Override
    public void setRowId(String rowIdFieldName, long rowId) {
        primaryDocumentInput.setRowId(rowIdFieldName, rowId);
        for (int i = 0; i < secondaryInputs.length; i++) {
            secondaryInputs[i].setRowId(rowIdFieldName, rowId);
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

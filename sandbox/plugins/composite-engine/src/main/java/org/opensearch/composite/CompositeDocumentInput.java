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
    private final Runnable onClose;

    /**
     * Constructs a CompositeDocumentInput with a primary format input and secondary format inputs.
     *
     * @param primaryFormat the primary data format
     * @param primaryDocumentInput the document input for the primary format
     * @param secondaryDocumentInputs a map of secondary data formats to their corresponding document inputs
     * @param onClose callback invoked when this composite input is closed, typically to release the writer back to the pool
     */
    public CompositeDocumentInput(
        DataFormat primaryFormat,
        DocumentInput<?> primaryDocumentInput,
        Map<DataFormat, DocumentInput<?>> secondaryDocumentInputs,
        Runnable onClose
    ) {
        this.primaryFormat = Objects.requireNonNull(primaryFormat, "primaryFormat must not be null");
        this.primaryDocumentInput = Objects.requireNonNull(primaryDocumentInput, "primaryDocumentInput must not be null");
        this.secondaryDocumentInputs = Map.copyOf(
            Objects.requireNonNull(secondaryDocumentInputs, "secondaryDocumentInputs must not be null")
        );
        this.onClose = Objects.requireNonNull(onClose, "onClose must not be null");
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
        for (Map.Entry<DataFormat, DocumentInput<?>> entry : secondaryDocumentInputs.entrySet()) {
            try {
                entry.getValue().addField(fieldType, value);
            } catch (Exception e) {
                throw new IllegalStateException(
                    "Failed to add field [" + fieldType.name() + "] in secondary format [" + entry.getKey().name() + "]",
                    e
                );
            }
        }
    }

    @Override
    public void setRowId(String rowIdFieldName, long rowId) {
        primaryDocumentInput.setRowId(rowIdFieldName, rowId);
        for (DocumentInput<?> input : secondaryDocumentInputs.values()) {
            input.setRowId(rowIdFieldName, rowId);
        }
    }

    @Override
    public List<? extends DocumentInput<?>> getFinalInput() {
        return null;
    }

    @Override
    public void close() {
        onClose.run();
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

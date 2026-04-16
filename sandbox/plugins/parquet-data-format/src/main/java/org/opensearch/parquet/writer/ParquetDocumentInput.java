/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Document input for the Parquet data format.
 *
 * <p>Implements {@link DocumentInput} to collect field-value pairs incrementally during
 * document indexing. Fields are stored as {@link FieldValuePair} objects and later transferred
 * to Arrow vectors by {@link org.opensearch.parquet.vsr.VSRManager#addDocument(ParquetDocumentInput)}.
 *
 * <p>Calling {@link #close()} clears all collected fields and resets the row ID,
 * allowing the instance to be discarded cleanly after use.
 */
public class ParquetDocumentInput implements DocumentInput<List<FieldValuePair>> {

    private final DataFormat owningFormat;
    private final List<FieldValuePair> collectedFields = new ArrayList<>();
    private long rowId = -1;
    private boolean isClosed = false;

    /**
     * Creates a new ParquetDocumentInput with the owning data format for capability filtering.
     *
     * @param owningFormat the DataFormat instance that owns this document input
     */
    public ParquetDocumentInput(DataFormat owningFormat) {
        this.owningFormat = Objects.requireNonNull(owningFormat, "owningFormat must not be null");
    }

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        ensureOpen();
        // Check capability map — accept only if this format owns capabilities for this field type
        Map<DataFormat, Set<FieldTypeCapabilities.Capability>> capMap = fieldType.getCapabilityMap();
        if (capMap.isEmpty()) {
            // No capability map set — no format declared support for this field type, skip it
            return;
        }

        Set<FieldTypeCapabilities.Capability> ownedCaps = capMap.get(owningFormat);
        if (ownedCaps != null && ownedCaps.isEmpty() == false) {
            collectedFields.add(new FieldValuePair(fieldType, value));
        }
        // else: silently skip — this format has no capabilities for this field type
    }

    @Override
    public void setRowId(String rowIdFieldName, long rowId) {
        ensureOpen();
        this.rowId = rowId;
    }

    @Override
    public List<FieldValuePair> getFinalInput() {
        if (!isClosed) {
            assert rowId >= 0 : "Row ID must be set before calling getFinalInput";
            // assertions for parquet primary
            // TODO: once parquet is supported in secondary mode, this assertion would change
            assert getFieldCount(IdFieldMapper.NAME) == 1;
            assert getFieldCount(SeqNoFieldMapper.NAME) == 1;
            assert getFieldCount(VersionFieldMapper.NAME) == 1;
            assert getFieldCount(SeqNoFieldMapper.PRIMARY_TERM_NAME) == 1;
        }
        return collectedFields;
    }

    @Override
    public long getFieldCount(String fieldName) {
        return collectedFields.stream().filter(fvp -> fvp.getFieldType().name().equals(fieldName)).count();
    }

    @Override
    public void close() {
        isClosed = true;
        collectedFields.clear();
        rowId = -1;
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException("Cannot add more fields to a frozen document input");
        }
    }

    /**
     * Returns the row ID assigned to this document.
     *
     * @return the row ID, or -1 if not set
     */
    public long getRowId() {
        return rowId;
    }
}

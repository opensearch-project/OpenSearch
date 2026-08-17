/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.exec.PrimaryTermFieldType;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.parquet.ParquetDataFormatPlugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
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

    private static final Logger logger = LogManager.getLogger(ParquetDocumentInput.class);
    private final List<FieldValuePair> collectedFields = new ArrayList<>();
    private final Set<MappedFieldType> dedup = Collections.newSetFromMap(new IdentityHashMap<>());
    /**
     * Engine-4: {@code nested} elements staged for this document, in source order. Each becomes a
     * position in the parallel {@code LIST<primitive>} leaf columns of the parent row; {@code VSRManager}
     * consumes them at {@code addDocument}. Empty for a document with no nested field.
     */
    private final List<NestedElement> nestedElements = new ArrayList<>();
    private long rowId = -1;
    private boolean isClosed = false;

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        ensureOpen();
        Set<FieldTypeCapabilities.Capability> capabilities = fieldType.getCapabilityMap()
            .getOrDefault(ParquetDataFormatPlugin.PARQUET_DATA_FORMAT, Set.of());
        if (capabilities.isEmpty() && fieldType != PrimaryTermFieldType.INSTANCE) {
            // nothing to support on this format for this field.
            logger.trace("Ignored to add field: {} {}", fieldType.name(), fieldType.getCapabilityMap());
            return;
        }
        if (dedup.add(fieldType) == false) {
            throw new MapperParsingException(
                "Cannot accept multiple values for field: [" + fieldType.name() + "] of type: [" + fieldType.typeName() + "]."
            );
        }
        collectedFields.add(new FieldValuePair(fieldType, value));
    }

    /**
     * Engine-4: stages one element of a {@code nested} array. Nothing is added to
     * {@link #collectedFields} — the element's leaf values are laid out as positions in the parent
     * row's parallel {@code LIST} columns by {@code VSRManager}, and its bridge offset/count are
     * derived there too, so a nested leaf never becomes a flat single-value field.
     */
    @Override
    public void addNestedElement(String nestedPath, int ordinal, List<MappedFieldType> fieldTypes, List<Object> values) {
        ensureOpen();
        assert fieldTypes.size() == values.size() : "fieldTypes and values must be parallel";
        List<String> leafNames = new ArrayList<>(fieldTypes.size());
        for (MappedFieldType fieldType : fieldTypes) {
            leafNames.add(fieldType.name());
        }
        nestedElements.add(new NestedElement(nestedPath, ordinal, List.copyOf(leafNames), List.copyOf(values)));
    }

    /** Nested elements staged for this document, in source order. Never null. */
    public List<NestedElement> getNestedElements() {
        return nestedElements;
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
        nestedElements.clear();
        rowId = -1;
    }

    /**
     * One element of a {@code nested} array staged for the parallel-LIST write path. {@code leafNames}
     * and {@code values} are parallel; a leaf absent from this element is simply not listed (it becomes
     * a null at this element's position in that leaf's list column).
     */
    public record NestedElement(String nestedPath, int ordinal, List<String> leafNames, List<Object> values) {}

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

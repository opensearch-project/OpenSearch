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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    // Keyed by field name, not field-type identity: within a single document parse each logical
    // field (including the derived-source `_ignored_source.*` companion) has a unique name, while
    // identity would silently miss a match if the parser ever handed back a fresh wrapper per array
    // element — degrading a multi_value field to last-value-wins or bypassing the scalar duplicate
    // guard. Name keying makes accumulation robust to that.
    private final Map<String, FieldValuePair> seen = new HashMap<>();
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
        FieldValuePair existing = seen.get(fieldType.name());
        if (existing == null) {
            // Fields declared `multi_value: true` in the mapping start out as a list of one so the
            // value shape reaching the VSR is the same whether the document had one value or several.
            // An explicit empty array (`"field": []`) is signalled by an empty List and seeds a
            // zero-value pair, so its LIST cell is written empty-but-non-null rather than null.
            final FieldValuePair pair;
            if (fieldType.isMultiValued()) {
                pair = value instanceof List<?> list && list.isEmpty()
                    ? FieldValuePair.emptyMultiValued(fieldType)
                    : FieldValuePair.multiValued(fieldType, value);
            } else {
                pair = new FieldValuePair(fieldType, value);
            }
            seen.put(fieldType.name(), pair);
            collectedFields.add(pair);
            return;
        }
        if (existing.isMultiValued() == false) {
            throw new MapperParsingException(
                "Cannot accept multiple values for field: ["
                    + fieldType.name()
                    + "] of type: ["
                    + fieldType.typeName()
                    + "]. Set [multi_value: true] on the field mapping to store multiple values."
            );
        }
        existing.addValue(value);
    }

    @Override
    public void setRowId(String rowIdFieldName, long rowId) {
        ensureOpen();
        this.rowId = rowId;
    }

    /**
     * Rejects a document whose correlated group has sub-fields of differing array length.
     * <p>
     * Each field is written as its own {@code LIST} column with independent offsets, so unequal
     * lengths mean index {@code i} of one field no longer describes the same element as index
     * {@code i} of its siblings — a document that writes cleanly and reads back mispaired. Mirrors
     * ClickHouse, which refuses such an insert into a {@code Nested} column rather than storing a
     * misaligned row.
     * <p>
     * Group membership comes from {@link MappedFieldType#correlationGroup()}, stamped at mapping build
     * time on the children of an object declared {@code nested} with {@code correlated: true}.
     * <p>
     * Only fields <em>present</em> in the document are compared. A sub-field the document never
     * mentions has nothing to mispair — every element of it is absent — so requiring it to be
     * present would reject legitimate documents (an event batch carrying no attributes at all).
     */
    private void validateCorrelatedGroups() {
        // Tracks the first present, multi-valued field seen per group and the count it established.
        Map<String, FieldValuePair> firstInGroup = null;
        for (FieldValuePair pair : collectedFields) {
            String group = pair.getFieldType().correlationGroup();
            if (group == null || pair.isMultiValued() == false) {
                continue;
            }
            if (firstInGroup == null) {
                firstInGroup = new HashMap<>();
            }
            FieldValuePair first = firstInGroup.putIfAbsent(group, pair);
            if (first == null) {
                continue;
            }
            int expected = first.valueCount();
            int count = pair.valueCount();
            if (count != expected) {
                throw new MapperParsingException(
                    "Fields in correlated group ["
                        + group
                        + "] must have the same number of values in a document, but ["
                        + first.getFieldType().name()
                        + "] has "
                        + expected
                        + " and ["
                        + pair.getFieldType().name()
                        + "] has "
                        + count
                        + ". Correlated arrays are paired by position, so differing lengths would "
                        + "associate values from different elements."
                );
            }
        }
    }

    @Override
    public List<FieldValuePair> getFinalInput() {
        if (!isClosed) {
            validateCorrelatedGroups();
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
        // Counts values, not entries: a multi-valued field is one entry holding N values, and
        // callers (single-value assertions below, the data-stream @timestamp check) mean values.
        return collectedFields.stream()
            .filter(fvp -> fvp.getFieldType().name().equals(fieldName))
            .mapToLong(FieldValuePair::valueCount)
            .sum();
    }

    @Override
    public void close() {
        isClosed = true;
        collectedFields.clear();
        seen.clear();
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

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
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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

    /**
     * Per-document metadata every child-table row inherits from its parent. {@code _id} and
     * {@code _seq_no} let a later delete/update fan out to the child rows by value; the parquet
     * document input additionally asserts all four are present on every row it admits.
     */
    private static final Set<String> INHERITED_METADATA_FIELDS = Set.of(
        IdFieldMapper.NAME,
        SeqNoFieldMapper.NAME,
        VersionFieldMapper.NAME,
        SeqNoFieldMapper.PRIMARY_TERM_NAME
    );

    private final DocumentInput<?> primaryDocumentInput;
    private final DataFormat primaryFormat;
    private final Map<DataFormat, DocumentInput<?>> secondaryDocumentInputs;
    private long rowId = -1L;
    /** Staged nested elements, in source order; empty for a document with no nested field. */
    private final List<NestedElement> nestedElements = new ArrayList<>();
    /** The subset of {@link #INHERITED_METADATA_FIELDS} seen on this document, in arrival order. */
    private final List<FieldValue> inheritedMetadata = new ArrayList<>();

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
        // Null-guarded rather than asserted: the engine sources metadata field types from the mapper
        // service ({@code DataFormatAwareEngine#indexIntoEngine}), which yields null for a metadata
        // field the mapping does not declare. The per-format inputs already accepted that before the
        // child table started watching for inherited metadata, so reading name() here unguarded turns
        // a tolerated case into a failed engine. A child row then inherits no seq_no, which the
        // delete fan-out will have to account for — but that is a later obligation, not a crash now.
        if (fieldType != null && INHERITED_METADATA_FIELDS.contains(fieldType.name())) {
            inheritedMetadata.add(new FieldValue(fieldType, value));
        }
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

    /**
     * Engine-4 (parallel LIST columns + element index): stages one element of a {@code nested} array
     * and broadcasts it to the primary format input. The primary (parquet) input lays the element's
     * leaves out as positions in the parent row's parallel {@code LIST} columns and derives the bridge
     * offset/count; the staged copy here feeds {@link CompositeWriter} the same elements for the
     * co-located element index. Secondaries (the main Lucene index) do not model nested elements, so
     * the broadcast is to the primary only.
     */
    @Override
    public void addNestedElement(String nestedPath, int ordinal, List<MappedFieldType> fieldTypes, List<Object> values) {
        assert fieldTypes.size() == values.size() : "fieldTypes and values must be parallel";
        nestedElements.add(new NestedElement(nestedPath, ordinal, List.copyOf(fieldTypes), List.copyOf(values)));
        primaryDocumentInput.addNestedElement(nestedPath, ordinal, fieldTypes, values);
    }

    /** Returns the nested elements staged for this document, in source order. Never null. */
    public List<NestedElement> getNestedElements() {
        return nestedElements;
    }

    /**
     * Returns the document metadata a child row inherits from its parent ({@code _id},
     * {@code _seq_no}, {@code _version}, {@code _primary_term}).
     */
    public List<FieldValue> getInheritedMetadata() {
        return inheritedMetadata;
    }

    /** A field type paired with its value. */
    public record FieldValue(MappedFieldType fieldType, Object value) {
    }

    /**
     * One element of a {@code nested} array, destined to become a row of the child table.
     * {@code fieldTypes} and {@code values} are parallel.
     */
    public record NestedElement(String nestedPath, int ordinal, List<MappedFieldType> fieldTypes, List<Object> values) {
    }
}

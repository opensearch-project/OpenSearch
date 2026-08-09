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
import java.util.HashSet;
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

    /** Default expected field count when the caller has no sizing hint. */
    static final int DEFAULT_EXPECTED_FIELDS = 16;

    private final List<FieldValuePair> collectedFields;
    /**
     * Detects duplicate single-valued fields, keyed by field <b>name</b>. Two {@code addField} calls with the
     * same name necessarily carry the same {@link MappedFieldType} instance — the mapping layer forbids two
     * mappers sharing a name (see {@code MappingLookup}) — so name-equality dedup is equivalent to the previous
     * object-identity dedup. Keying on the name String rather than the field-type object avoids
     * {@code System.identityHashCode} on the bulk-indexing hot path: {@code MappedFieldType} inherits
     * {@code Object.hashCode()} (identity → {@code JVM_IHashCode}), whereas {@code String.hashCode()} is cached.
     * O(n) in field count, so no quadratic blow-up on wide documents.
     */
    private final Set<String> seenFieldNames;
    private long rowId = -1;
    private boolean isClosed = false;

    /** Creates a document input with the default sizing hint. */
    public ParquetDocumentInput() {
        this(DEFAULT_EXPECTED_FIELDS);
    }

    /**
     * Creates a document input pre-sized for the expected number of fields.
     * <p>
     * One instance is allocated per document at bulk-indexing rates, and the dedup set's incremental
     * {@code HashMap} table resizes (16 → ... → 256 for a ~100-field document) were a measurable slice of an
     * ingest allocation profile. Pre-sizing from the mapped-field count eliminates the resize copies; callers
     * with no hint get the default.
     *
     * @param expectedFields expected number of fields this document will carry (typically the mapped field
     *                       count of the index; an upper bound is fine, values below 1 fall back to the default)
     */
    public ParquetDocumentInput(int expectedFields) {
        int expected = expectedFields > 0 ? expectedFields : DEFAULT_EXPECTED_FIELDS;
        this.collectedFields = new ArrayList<>(expected);
        // HashSet resizes above capacity * 0.75; size so `expected` inserts never trigger a resize.
        this.seenFieldNames = new HashSet<>(expected * 4 / 3 + 1);
    }

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
        if (seenFieldNames.add(fieldType.name()) == false) {
            throw new MapperParsingException(
                "Cannot accept multiple values for field: [" + fieldType.name() + "] of type: [" + fieldType.typeName() + "]."
            );
        }
        collectedFields.add(new FieldValuePair(fieldType, value));
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
        seenFieldNames.clear();
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

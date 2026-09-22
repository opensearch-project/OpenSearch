/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.NestedParquetField;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;
import org.opensearch.parquet.writer.ParquetDocumentInput;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parquet field for {@code flat_object} — an open, dynamic key space stored as one
 * {@code MAP<Utf8,Utf8>} column.
 *
 * <p>This registration exists so the parquet data format <em>advertises</em> that it can serve a
 * {@code flat_object} field (capability coverage in {@code CompositeDataFormatPlugin}; without it the
 * field's requested capabilities go unclaimed and index creation is rejected). Values arrive as
 * {@code Map.Entry}-valued {@code DocumentInput.addField} calls (see {@code FlatObjectFieldMapper}),
 * buffered by {@code ParquetDocumentInput} and written into a {@code MapVector} by
 * {@link #writeMapChild}/{@link #writeTopLevelMaps} below — NOT through {@link #addToGroup}.
 *
 * <p>{@link #addToGroup} is therefore never invoked on this field and throws defensively if it ever is.
 * It could not serve the nested case anyway: the struct-child write path resolves children itself and
 * never consults {@link ParquetField}.
 */
public class FlatObjectParquetField extends ParquetField {

    /** Creates a new FlatObjectParquetField. */
    public FlatObjectParquetField() {}

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        throw new UnsupportedOperationException(
            "flat_object [" + mappedFieldType.name() + "] is written via writeMapChild/MapVector, not addToGroup"
        );
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Map(false);
    }

    @Override
    public Set<FieldTypeCapabilities.Capability> supportedCapabilities() {
        // No FULL_TEXT_SEARCH: FlatObjectFieldType.requestedCapabilities() excludes it in pluggable
        // mode, so it's never requested here. No BLOOM_FILTER: it would have to target the nested
        // key/value leaves, which field-level settings cannot address.
        return Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE);
    }

    @Override
    public FieldType getFieldType() {
        // Nominal type only — the real MAP<Utf8,Utf8> field (with key_value/key/value children) is
        // built by buildField below.
        return FieldType.nullable(getArrowType());
    }

    /**
     * Builds the real {@code MAP<Utf8,Utf8>} field — {@code Map("key_value": non-null Struct("key":
     * non-null Utf8, "value": nullable Utf8), unsorted)} — matching the canonical parquet MAP layout
     * rather than Arrow Java's default {@code entries} group name, so the arrow-rs writer and the
     * DataFusion read path see the group name the parquet spec prescribes. Called uniformly by schema
     * building whether {@code name} is a document-root field or a leaf name inside a nested struct, so
     * a flat_object gets the same shape either way.
     */
    @Override
    public Field buildField(String name) {
        Field key = new Field("key", new FieldType(false, ArrowType.Utf8.INSTANCE, null), null);
        Field value = new Field("value", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        Field entries = new Field("key_value", new FieldType(false, ArrowType.Struct.INSTANCE, null), List.of(key, value));
        return new Field(name, FieldType.nullable(getArrowType()), List.of(entries));
    }

    /**
     * Writes document-root MAP columns (a top-level {@code flat_object}) at {@code rowIndex}. Iterates
     * every top-level MAP vector so each row's value is set explicitly — {@code null} when the document
     * has no entries for that field — instead of leaving skipped rows to Arrow's implicit offset
     * back-fill (which would otherwise leave the slot in whatever state a prior row's write left it in).
     */
    public void writeTopLevelMaps(ParquetDocumentInput doc, ManagedVSR activeVSR, int rowIndex) {
        for (Field field : activeVSR.getSchema().getFields()) {
            if (activeVSR.getVector(field.getName()) instanceof MapVector mapVector) {
                writeMapChild(mapVector, rowIndex, doc.getTopLevelMapEntries().getOrDefault(mapVector.getName(), List.of()));
            }
        }
    }

    /**
     * Writes one {@code MAP<Utf8,Utf8>} value at {@code index}: each buffered (key,value) becomes one
     * map entry; a null value leaves the entry's value null. An EMPTY entry list writes an explicit
     * {@code null} for the whole map, not an empty non-null one — a flat_object with no entries must
     * round-trip as absent, matching classic OpenSearch's {@code exists}/derived-source semantics. Also
     * called directly by {@link NestedParquetField} to write a flat_object MAP child inside a nested
     * element's struct.
     */
    public void writeMapChild(MapVector mapVector, int index, List<Map.Entry<String, Object>> entries) {
        if (entries.isEmpty()) {
            mapVector.setNull(index);
            return;
        }
        int start = mapVector.startNewValue(index);
        StructVector entriesStruct = (StructVector) mapVector.getDataVector();
        VarCharVector keyVector = (VarCharVector) entriesStruct.getChild(MapVector.KEY_NAME);
        VarCharVector valueVector = (VarCharVector) entriesStruct.getChild(MapVector.VALUE_NAME);
        for (int i = 0; i < entries.size(); i++) {
            int pos = start + i;
            entriesStruct.setIndexDefined(pos);
            Map.Entry<String, Object> entry = entries.get(i);
            keyVector.setSafe(pos, entry.getKey().getBytes(StandardCharsets.UTF_8));
            Object value = entry.getValue();
            if (value != null) {
                valueVector.setSafe(pos, value.toString().getBytes(StandardCharsets.UTF_8));
            }
        }
        mapVector.endValue(index, entries.size());
    }
}

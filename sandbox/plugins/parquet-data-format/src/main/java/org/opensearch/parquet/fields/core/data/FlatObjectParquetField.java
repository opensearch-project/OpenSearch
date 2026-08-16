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
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parquet field for {@code flat_object} values, stored as a single Arrow {@code MAP<utf8, utf8>}
 * column ({@link MapVector}) that Parquet writes as a {@code MAP} logical type over a repeated
 * {@code entries: STRUCT<key, value>} group.
 *
 * <p>One document contributes one map cell holding however many leaves its object had, so the
 * attribute set stays in a single column no matter how many distinct keys the index sees. This is why
 * the field is single-arity and does not support {@code multi_value}: a {@code LIST<MAP>} has no
 * meaning here, and the per-document entry count is already variable.
 *
 * <p>Values arrive from {@code FlatObjectFieldMapper} as an ordered {@code List<Map.Entry>} of
 * {@code (relative path, value)} pairs — see
 * {@code FlatObjectFieldMapper#parseCreateFieldForPluggableFormat}. Everything is stored as UTF-8
 * text, mirroring flat_object's Lucene representation, which also stringifies numbers and booleans.
 * Document order and duplicate keys are preserved: a Parquet MAP is physically a repeated group, so
 * {@code {"a": [1, 2]}} keeps both {@code a} entries rather than collapsing them.
 */
public class FlatObjectParquetField extends ParquetField {

    /** Creates a new FlatObjectParquetField. */
    public FlatObjectParquetField() {}

    /**
     * Writes one map cell at the current row.
     * <p>
     * A null value is written as a null cell, which is how an absent field is represented. An empty
     * entry list becomes a zero-entry, non-null map, keeping {@code "attrs": {}} distinct from no
     * {@code attrs} at all.
     */
    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        MapVector mapVector = (MapVector) managedVSR.getVector(mappedFieldType.name());
        int row = managedVSR.getRowCount();
        if (parseValue == null) {
            mapVector.setNull(row);
            return;
        }
        List<?> entries = (List<?>) parseValue;
        StructVector entriesVector = (StructVector) mapVector.getDataVector();
        VarCharVector keyVector = (VarCharVector) entriesVector.getChild(MapVector.KEY_NAME);
        VarCharVector valueVector = (VarCharVector) entriesVector.getChild(MapVector.VALUE_NAME);
        int start = mapVector.startNewValue(row);
        for (int i = 0; i < entries.size(); i++) {
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) entries.get(i);
            int index = start + i;
            // The entries struct is non-nullable per the Arrow MAP spec, so every slot written must
            // have its validity bit set; without this the child values read back as null.
            entriesVector.setIndexDefined(index);
            keyVector.setSafe(index, entry.getKey().toString().getBytes(StandardCharsets.UTF_8));
            Object value = entry.getValue();
            if (value == null) {
                valueVector.setNull(index);
            } else {
                valueVector.setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
            }
        }
        mapVector.endValue(row, entries.size());
    }

    /**
     * Builds the {@code entries: STRUCT<key, value>} child the Arrow MAP type requires. Per the Arrow
     * spec the entries struct and its {@code key} are non-nullable; only {@code value} may be null.
     */
    @Override
    protected List<Field> getChildren() {
        Field key = new Field(MapVector.KEY_NAME, FieldType.notNullable(new ArrowType.Utf8()), null);
        Field value = new Field(MapVector.VALUE_NAME, FieldType.nullable(new ArrowType.Utf8()), null);
        Field entries = new Field(MapVector.DATA_VECTOR_NAME, FieldType.notNullable(ArrowType.Struct.INSTANCE), List.of(key, value));
        return List.of(entries);
    }

    @Override
    public ArrowType getArrowType() {
        // keysSorted = false: entries keep document order rather than being sorted by key.
        return new ArrowType.Map(false);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }

    @Override
    public Set<FieldTypeCapabilities.Capability> supportedCapabilities() {
        // Matches what FlatObjectFieldType requests: it is searchable and has doc values, and the MAP
        // column serves both from the same storage. No BLOOM_FILTER — a bloom filter would have to be
        // configured against the nested key/value leaves, which field-level settings cannot address.
        return Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH);
    }
}

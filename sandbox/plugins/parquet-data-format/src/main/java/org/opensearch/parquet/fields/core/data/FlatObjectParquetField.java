/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

import java.util.List;
import java.util.Set;

/**
 * Parquet field for {@code flat_object} — an open, dynamic key space stored as one
 * {@code MAP<Utf8,Utf8>} column.
 *
 * <p>This registration exists so the parquet data format <em>advertises</em> that it can serve a
 * {@code flat_object} field (capability coverage in {@code CompositeDataFormatPlugin}; without it the
 * field's requested capabilities go unclaimed and index creation is rejected). Values are written via
 * the {@code addMapEntry} signal into a {@code MapVector} by {@code VSRManager} — NOT through
 * {@link #addToGroup}.
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
        // flat_object values arrive as map entries (DocumentInput.addMapEntry) and are written to a
        // MapVector by VSRManager, not through the scalar createField path.
        throw new UnsupportedOperationException(
            "flat_object [" + mappedFieldType.name() + "] is written via addMapEntry/MapVector, not addToGroup"
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
}

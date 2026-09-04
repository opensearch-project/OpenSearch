/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

import java.util.Set;

/**
 * Parquet field for {@code flat_object} — an open, dynamic key space stored as one
 * {@code MAP<Utf8,Utf8>} column.
 *
 * <p>This registration exists so the parquet data format <em>advertises</em> that it can serve a
 * {@code flat_object} field (capability coverage in {@code CompositeDataFormatPlugin}; without it the
 * field's requested capabilities go unclaimed and index creation is rejected). The actual Arrow schema
 * for a flat_object is built by {@code ArrowSchemaBuilder.buildMapField} (which bypasses this registry
 * so the same shape is produced at the document root and inside a nested element's struct), and values
 * are written via the {@code addMapEntry} signal into a {@code MapVector} by {@code VSRManager} — NOT
 * through {@link #addToGroup}.
 *
 * <p>{@link #addToGroup} is therefore never invoked on this field and throws defensively if it ever is.
 * It could not serve the nested case anyway: the struct-child write path
 * ({@code VSRManager.writeChildList}) resolves children itself and never consults {@link ParquetField}.
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
        // Parquet owns the field outright: it must claim every capability a searchable + doc-valued
        // flat_object requests (FULL_TEXT_SEARCH from isSearchable, COLUMNAR_STORAGE from doc values),
        // otherwise assignCapabilities leaves one unclaimed and rejects index creation.
        // No BLOOM_FILTER: it would have to target the nested key/value leaves, which field-level
        // settings cannot address.
        return Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH);
    }

    @Override
    public FieldType getFieldType() {
        // Nominal type only — the real MAP<Utf8,Utf8> field (with key_value/key/value children) is built
        // by ArrowSchemaBuilder.buildMapField, which special-cases flat_object before the registry.
        return FieldType.nullable(getArrowType());
    }
}

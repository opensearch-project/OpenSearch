/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.metadata;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * Parquet field for _routing metadata stored as UTF-8 using {@link VarCharVector}.
 */
public class RoutingParquetField extends ParquetField {

    /** Creates a new RoutingParquetField. */
    public RoutingParquetField() {}

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        ((VarCharVector) managedVSR.getVector(mappedFieldType.name())).setSafe(
            managedVSR.getRowCount(),
            parseValue.toString().getBytes(StandardCharsets.UTF_8)
        );
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Utf8();
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }

    @Override
    public Set<FieldTypeCapabilities.Capability> supportedCapabilities() {
        return Set.of(
            FieldTypeCapabilities.Capability.COLUMNAR_STORAGE,
            FieldTypeCapabilities.Capability.BLOOM_FILTER,
            FieldTypeCapabilities.Capability.STORED_FIELDS,
            FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH
        );
    }
}

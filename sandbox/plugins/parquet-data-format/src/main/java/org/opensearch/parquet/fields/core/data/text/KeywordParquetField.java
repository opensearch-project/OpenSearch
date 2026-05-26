/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.text;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

import java.nio.charset.StandardCharsets;

/**
 * Parquet field for keyword values using {@link VarCharVector} with UTF-8 encoding.
 */
public class KeywordParquetField extends ParquetField {

    /** Creates a new KeywordParquetField. */
    public KeywordParquetField() {}

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
}

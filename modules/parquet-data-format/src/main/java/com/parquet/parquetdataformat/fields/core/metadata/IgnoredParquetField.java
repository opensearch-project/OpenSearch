/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields.core.metadata;

import com.parquet.parquetdataformat.fields.ParquetField;
import com.parquet.parquetdataformat.vsr.ManagedVSR;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

import java.nio.charset.StandardCharsets;

/**
 * Parquet field implementation for handling ignored field data types in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch ignored fields and Apache Arrow
 * UTF-8 string vectors for columnar storage in Parquet format. Ignored field values are stored
 * using Apache Arrow's {@link VarCharVector}, which provides efficient variable-length string storage.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code ignored} field mapping and
 * supports fields that are indexed but not stored in the document source. The field values
 * are converted to UTF-8 string representation before storage in the Arrow vector.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * IgnoredParquetField ignoredField = new IgnoredParquetField();
 * ArrowType arrowType = ignoredField.getArrowType(); // Returns UTF-8 string type
 * FieldType fieldType = ignoredField.getFieldType(); // Returns nullable UTF-8 field type
 * }</pre>
 *
 * @see ParquetField
 * @see VarCharVector
 * @see ArrowType.Utf8
 * @since 1.0
 */
public class IgnoredParquetField extends ParquetField {

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        VarCharVector varCharVector = (VarCharVector) managedVSR.getVector(mappedFieldType.name());
        int rowIndex = managedVSR.getRowCount();
        varCharVector.setSafe(rowIndex, parseValue.toString().getBytes(StandardCharsets.UTF_8));
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

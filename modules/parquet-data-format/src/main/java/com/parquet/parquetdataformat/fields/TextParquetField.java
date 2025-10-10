/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields;

import com.parquet.parquetdataformat.vsr.ManagedVSR;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

import java.nio.charset.StandardCharsets;

/**
 * Parquet field implementation for handling text data types in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch text fields and Apache Arrow
 * vectors for columnar storage in Parquet format. Text values are stored using Apache Arrow's
 * {@link VarCharVector}, which provides efficient variable-length string storage with UTF-8 encoding.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code text} field mapping, which is
 * typically used for full-text search operations. Text fields are usually analyzed during
 * indexing, but this implementation stores the original text content for columnar access.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * TextParquetField textField = new TextParquetField();
 * ArrowType arrowType = textField.getArrowType(); // Returns ArrowType.Utf8
 * FieldType fieldType = textField.getFieldType(); // Returns non-nullable integer field type
 * }</pre>
 *
 * @see ParquetField
 * @see ArrowFieldRegistry
 * @see VarCharVector
 * @see ArrowType.Int
 * @since 1.0
 */
public class TextParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        VarCharVector textVector = (VarCharVector) managedVSR.getVector(mappedFieldType.name());
        int rowIndex = managedVSR.getRowCount();
        textVector.setSafe(rowIndex, parseValue.toString().getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Utf8();
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.notNullable(getArrowType());
    }
}

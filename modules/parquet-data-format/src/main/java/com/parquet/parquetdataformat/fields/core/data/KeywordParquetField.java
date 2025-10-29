/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields.core.data;

import com.parquet.parquetdataformat.fields.ParquetField;
import com.parquet.parquetdataformat.vsr.ManagedVSR;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

import java.nio.charset.StandardCharsets;

/**
 * Parquet field implementation for handling keyword data types in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch keyword fields and Apache Arrow
 * UTF-8 string vectors for columnar storage in Parquet format. Keyword values are stored using
 * Apache Arrow's {@link VarCharVector}, which provides efficient variable-length string storage
 * with UTF-8 encoding.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code keyword} field mapping, which is
 * typically used for exact-match searches, aggregations, and sorting. Unlike text fields,
 * keyword fields are not analyzed and are stored as-is for precise matching.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * KeywordParquetField keywordField = new KeywordParquetField();
 * ArrowType arrowType = keywordField.getArrowType(); // Returns ArrowType.Utf8
 * FieldType fieldType = keywordField.getFieldType(); // Returns non-nullable UTF-8 field type
 * }</pre>
 *
 * @see ParquetField
 * @see VarCharVector
 * @see ArrowType.Utf8
 * @since 1.0
 */
public class KeywordParquetField extends ParquetField {

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
        return FieldType.nullable(getArrowType());
    }
}

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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Parquet field implementation for handling token count data types in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch token count fields and Apache Arrow
 * 32-bit signed integer vectors for columnar storage in Parquet format. Token count values are stored
 * using Apache Arrow's {@link IntVector}, which provides efficient storage for signed integer values
 * representing the number of tokens in analyzed text fields.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code token_count} field mapping, which is used
 * for storing the count of tokens produced by text analysis. This field is particularly useful
 * for implementing token-based queries, analyzing text complexity, and performing aggregations
 * based on document length in terms of token count.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * TokenCountParquetField tokenCountField = new TokenCountParquetField();
 * ArrowType arrowType = tokenCountField.getArrowType(); // Returns ArrowType.Int(32, true)
 * FieldType fieldType = tokenCountField.getFieldType(); // Returns non-nullable 32-bit signed integer field type
 * }</pre>
 *
 * @see ParquetField
 * @see IntVector
 * @see ArrowType.Int
 * @since 1.0
 */
public class TokenCountParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        IntVector intVector = (IntVector) managedVSR.getVector(mappedFieldType.name());
        int rowCount = managedVSR.getRowCount();
        intVector.setSafe(rowCount, (Integer) parseValue);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Int(32, true);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }
}

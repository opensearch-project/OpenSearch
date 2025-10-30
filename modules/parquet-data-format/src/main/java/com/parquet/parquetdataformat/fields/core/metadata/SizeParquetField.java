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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Parquet field implementation for handling document size metadata in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch document size fields and Apache Arrow
 * 32-bit signed integer vectors for columnar storage in Parquet format. Document size values are stored
 * using Apache Arrow's {@link IntVector}, which provides efficient storage for signed integer values
 * representing the size of documents in bytes.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code _size} metadata field, which is used
 * for storing the size of the original document source in bytes. This field is particularly useful
 * for monitoring storage usage, implementing size-based queries, and analyzing document distribution
 * by size across indices.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * SizeParquetField sizeField = new SizeParquetField();
 * ArrowType arrowType = sizeField.getArrowType(); // Returns ArrowType.Int(32, true)
 * FieldType fieldType = sizeField.getFieldType(); // Returns non-nullable 32-bit signed integer field type
 * }</pre>
 *
 * @see ParquetField
 * @see IntVector
 * @see ArrowType.Int
 * @since 1.0
 */
public class SizeParquetField extends ParquetField {

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

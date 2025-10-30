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
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Parquet field implementation for handling binary data types in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch binary fields and Apache Arrow
 * variable-length binary vectors for columnar storage in Parquet format. Binary values are stored using
 * Apache Arrow's {@link VarBinaryVector}, which handles variable-length byte arrays.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code binary} field mapping and supports
 * arbitrary byte sequences. All binary data is stored as-is without transformation.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * BinaryParquetField binaryField = new BinaryParquetField();
 * ArrowType arrowType = binaryField.getArrowType(); // Returns Binary type
 * FieldType fieldType = binaryField.getFieldType(); // Returns nullable binary field type
 * }</pre>
 *
 * @see ParquetField
 * @see VarBinaryVector
 * @see ArrowType.Binary
 * @since 1.0
 */
public class BinaryParquetField extends ParquetField {

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        final VarBinaryVector varBinaryVector = (VarBinaryVector) managedVSR.getVector(mappedFieldType.name());
        int rowCount = managedVSR.getRowCount();
        varBinaryVector.set(rowCount, (byte[]) parseValue);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Binary();
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }
}

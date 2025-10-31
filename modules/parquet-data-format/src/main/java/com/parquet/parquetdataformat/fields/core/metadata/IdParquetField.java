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
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Parquet field implementation for handling document ID metadata in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch document ID fields and Apache Arrow
 * UTF-8 string vectors for columnar storage in Parquet format. Document ID values are stored
 * using Apache Arrow's {@link VarCharVector}, which provides efficient variable-length string storage.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code _id} metadata field and
 * supports unique document identifiers. The ID values are processed from {@link BytesRef} objects
 * and stored directly in the Arrow vector with proper offset and length handling.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * IdParquetField idField = new IdParquetField();
 * ArrowType arrowType = idField.getArrowType(); // Returns UTF-8 string type
 * FieldType fieldType = idField.getFieldType(); // Returns nullable UTF-8 field type
 * }</pre>
 *
 * @see ParquetField
 * @see VarCharVector
 * @see ArrowType.Utf8
 * @since 1.0
 */
public class IdParquetField extends ParquetField {

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        VarCharVector idVector = (VarCharVector) managedVSR.getVector(mappedFieldType.name());
        int rowIndex = managedVSR.getRowCount();
        BytesRef bytesRef = (BytesRef) parseValue;
        idVector.setSafe(rowIndex, bytesRef.bytes, bytesRef.offset, bytesRef.length);
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

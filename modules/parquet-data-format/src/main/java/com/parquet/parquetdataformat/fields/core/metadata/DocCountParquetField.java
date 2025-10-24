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
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Parquet field implementation for handling document count metadata in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch document count fields and Apache Arrow
 * unsigned 64-bit integer vectors for columnar storage in Parquet format. Document count values are stored
 * using Apache Arrow's {@link UInt8Vector}, which provides efficient storage for unsigned integer values
 * representing the number of documents or occurrences.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code _doc_count} metadata field, which is used
 * for storing aggregated document counts in pre-aggregated indices or rollup operations. The field
 * stores non-negative integer values representing document frequencies or counts.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * DocCountParquetField docCountField = new DocCountParquetField();
 * ArrowType arrowType = docCountField.getArrowType(); // Returns ArrowType.Int(64, false)
 * FieldType fieldType = docCountField.getFieldType(); // Returns non-nullable 64-bit integer field type
 * }</pre>
 *
 * @see ParquetField
 * @see UInt8Vector
 * @see ArrowType.Int
 * @since 1.0
 */
public class DocCountParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        UInt8Vector uInt8Vector = (UInt8Vector) managedVSR.getVector(mappedFieldType.name());
        int rowCount = managedVSR.getRowCount();
        long longValue = ((Number) parseValue).longValue();
        uInt8Vector.setSafe(rowCount, longValue);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Int(64, false);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.notNullable(getArrowType());
    }
}

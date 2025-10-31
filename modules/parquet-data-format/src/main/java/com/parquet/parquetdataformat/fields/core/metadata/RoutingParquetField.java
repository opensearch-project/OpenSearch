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
 * Parquet field implementation for handling routing metadata in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch routing fields and Apache Arrow
 * UTF-8 string vectors for columnar storage in Parquet format. Routing values are stored
 * using Apache Arrow's {@link VarCharVector}, which provides efficient variable-length string storage.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code _routing} metadata field and
 * supports custom routing values that determine which shard a document is stored on. The routing
 * value is converted to UTF-8 bytes before storage in the Arrow vector.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * RoutingParquetField routingField = new RoutingParquetField();
 * ArrowType arrowType = routingField.getArrowType(); // Returns UTF-8 string type
 * FieldType fieldType = routingField.getFieldType(); // Returns nullable UTF-8 field type
 * }</pre>
 *
 * @see ParquetField
 * @see VarCharVector
 * @see ArrowType.Utf8
 * @since 1.0
 */
public class RoutingParquetField extends ParquetField {

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        VarCharVector routingVector = (VarCharVector) managedVSR.getVector(mappedFieldType.name());
        int rowIndex = managedVSR.getRowCount();
        routingVector.setSafe(rowIndex, parseValue.toString().getBytes(StandardCharsets.UTF_8));
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

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

public class TextParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        VarCharVector textVector = (VarCharVector) managedVSR.getVector(mappedFieldType.name());
        int rowIndex = managedVSR.getRowCount();
        textVector.setSafe(rowIndex, parseValue.toString().getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Int(8, true);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.notNullable(getArrowType());
    }
}

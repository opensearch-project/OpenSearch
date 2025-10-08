/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields.number;

import com.parquet.parquetdataformat.fields.ParquetField;
import com.parquet.parquetdataformat.vsr.ManagedVSR;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

public class UnsignedLongParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        UInt8Vector uInt8Vector = (UInt8Vector) managedVSR.getVector(mappedFieldType.name());
        int rowCount = managedVSR.getRowCount();
        if (parseValue == null) {
            uInt8Vector.setNull(rowCount);
        } else {
            long longValue = ((Number) parseValue).longValue();
            uInt8Vector.setSafe(rowCount, longValue);
        }
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

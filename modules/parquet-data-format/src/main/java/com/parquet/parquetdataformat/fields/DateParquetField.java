/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields;

import com.parquet.parquetdataformat.vsr.ManagedVSR;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

public class DateParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        DateMilliVector dateMilliVector = (DateMilliVector) managedVSR.getVector(mappedFieldType.name());
        int rowIndex = managedVSR.getRowCount();
        dateMilliVector.setSafe(rowIndex, (long) parseValue);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.notNullable(getArrowType());
    }
}

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
import org.apache.arrow.vector.Float8Vector;
import org.opensearch.index.mapper.MappedFieldType;

public class DoubleParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        Float8Vector float8Vector = (Float8Vector) managedVSR.getVector(mappedFieldType.name());
        int rowCount = managedVSR.getRowCount();
        if (parseValue == null) {
            float8Vector.setNull(rowCount);
        } else {
            float8Vector.setSafe(rowCount, (Double) parseValue);
        }
    }
}

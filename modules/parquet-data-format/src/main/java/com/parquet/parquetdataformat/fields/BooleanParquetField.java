/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields;

import com.parquet.parquetdataformat.vsr.ManagedVSR;
import org.apache.arrow.vector.BitVector;
import org.opensearch.index.mapper.MappedFieldType;

public class BooleanParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        BitVector bitVector = (BitVector) managedVSR.getVector(mappedFieldType.name());
        int rowIndex = managedVSR.getRowCount();
        bitVector.setSafe(rowIndex, (Boolean) parseValue ? 1 : 0);
    }
}

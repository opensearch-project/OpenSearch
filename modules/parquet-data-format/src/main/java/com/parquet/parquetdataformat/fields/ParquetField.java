/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields;

import com.parquet.parquetdataformat.vsr.ManagedVSR;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.index.mapper.MappedFieldType;

public abstract class ParquetField {
    public abstract void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue);

    public void createField(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        if (mappedFieldType.isColumnar()) {
            addToGroup(mappedFieldType, managedVSR, parseValue);
        }
    }
}

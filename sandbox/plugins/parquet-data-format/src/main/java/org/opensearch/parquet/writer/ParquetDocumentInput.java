/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Document input for the Parquet data format.
 *
 * <p>Collects fields incrementally as {@link FieldValuePair} objects,
 * decoupled from ManagedVSR. Fields are transferred to a VSR only
 * when {@link #transferFieldsToVSR(ManagedVSR)} is called.
 */
public class ParquetDocumentInput implements DocumentInput<List<FieldValuePair>> {

    private final List<FieldValuePair> collectedFields = new ArrayList<>();
    private long rowId = -1;

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        collectedFields.add(new FieldValuePair(fieldType, value));
    }

    @Override
    public void setRowId(String rowIdFieldName, long rowId) {
        this.rowId = rowId;
    }

    @Override
    public List<FieldValuePair> getFinalInput() {
        return collectedFields;
    }

    /**
     * Transfers collected fields into the given ManagedVSR using the ArrowFieldRegistry
     * to resolve typed vector writes.
     */
    public void transferFieldsToVSR(ManagedVSR managedVSR) throws IOException {
        for (FieldValuePair pair : collectedFields) {
            MappedFieldType fieldType = pair.getFieldType();
            if (fieldType == null) {
                continue;
            }
            ParquetField parquetField = ArrowFieldRegistry.getParquetField(fieldType.typeName());
            if (parquetField == null) {
                continue;
            }
            parquetField.createField(fieldType, managedVSR, pair.getValue());
        }
        managedVSR.setRowCount(managedVSR.getRowCount() + 1);
    }

    @Override
    public void close() {}
}

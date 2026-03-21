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

import java.util.ArrayList;
import java.util.List;

/**
 * Document input for the Parquet data format.
 *
 * <p>Collects fields incrementally as {@link FieldValuePair} objects.
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

    @Override
    public void close() {
        collectedFields.clear();
        rowId = -1;
    }
    public long getRowId() {
        return rowId;
    }
}

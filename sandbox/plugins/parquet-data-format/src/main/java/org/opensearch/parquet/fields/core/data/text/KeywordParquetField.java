/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.text;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Parquet field for keyword values using {@link VarCharVector} with UTF-8 encoding.
 */
public class KeywordParquetField extends ParquetField {

    /** Creates a new KeywordParquetField. */
    public KeywordParquetField() {}

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        FieldVector vector = managedVSR.getVector(mappedFieldType.name());
        int rowIndex = managedVSR.getRowCount();
        if (vector instanceof ListVector listVector) {
            writeList(listVector, rowIndex, parseValue);
        } else {
            writeValue((VarCharVector) vector, rowIndex, parseValue);
        }
    }

    private static void writeList(ListVector listVector, int rowIndex, Object parseValue) {
        if (parseValue == null) {
            listVector.setNull(rowIndex);
            return;
        }
        List<?> values = parseValue instanceof List<?> list ? list : List.of(parseValue);
        int start = listVector.startNewValue(rowIndex);
        VarCharVector dataVector = (VarCharVector) listVector.getDataVector();
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (value == null) {
                dataVector.setNull(start + i);
            } else {
                writeValue(dataVector, start + i, value);
            }
        }
        listVector.endValue(rowIndex, values.size());
    }

    private static void writeValue(VarCharVector vector, int index, Object value) {
        vector.setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public boolean supportsMultiValue() {
        return true;
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

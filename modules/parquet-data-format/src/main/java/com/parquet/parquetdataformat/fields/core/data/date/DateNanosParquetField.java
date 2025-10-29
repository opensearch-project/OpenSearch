/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields.core.data.date;

import com.parquet.parquetdataformat.fields.ParquetField;
import com.parquet.parquetdataformat.vsr.ManagedVSR;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Parquet field implementation for handling date and timestamp data types in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch date fields and Apache Arrow
 * timestamp vectors for columnar storage in Parquet format. Date values are stored using
 * Apache Arrow's {@link TimeStampNanoVector}, which stores timestamps as nanoseconds since the
 * Unix epoch (January 1, 1970, 00:00:00 UTC).</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code date_nanos} field mapping and supports
 * various date formats as configured in the field mapping. All dates are normalized to
 * nanosecond timestamps before storage in the Arrow vector.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * DateParquetField dateField = new DateParquetField();
 * ArrowType arrowType = dateField.getArrowType(); // Returns Timestamp with NANOSECOND precision
 * FieldType fieldType = dateField.getFieldType(); // Returns non-nullable timestamp field type
 * }</pre>
 *
 * @see ParquetField
 * @see TimeStampNanoVector
 * @see ArrowType.Timestamp
 * @since 1.0
 */
public class DateNanosParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        TimeStampNanoVector timeStampNanoVector = (TimeStampNanoVector) managedVSR.getVector(mappedFieldType.name());
        int rowIndex = managedVSR.getRowCount();
        timeStampNanoVector.setSafe(rowIndex, (long) parseValue);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }
}

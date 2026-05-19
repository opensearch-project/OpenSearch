/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.date;

import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

/**
 * Parquet field for date_nanos values stored as nanosecond timestamps using {@link TimeStampNanoVector}.
 */
public class DateNanosParquetField extends ParquetField {

    /** Creates a new DateNanosParquetField. */
    public DateNanosParquetField() {}

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        ((TimeStampNanoVector) managedVSR.getVector(mappedFieldType.name())).setSafe(managedVSR.getRowCount(), (long) parseValue);
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

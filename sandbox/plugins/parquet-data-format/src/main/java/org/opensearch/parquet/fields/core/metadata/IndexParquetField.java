package org.opensearch.parquet.fields.core.metadata;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

/**
 * Parquet field for index name.
 * This is a placeholder for declaring support for the field.
 */
public class IndexParquetField extends ParquetField {

    @Override
    protected void addToGroup(MappedFieldType fieldType, ManagedVSR managedVSR, Object parseValue) {
        throw new IllegalStateException("Index field is not supposed to be added to group");
    }

    @Override
    public ArrowType getArrowType() {
        return null;
    }

    @Override
    public FieldType getFieldType() {
        return null;
    }
}

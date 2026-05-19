package org.opensearch.parquet.fields.core.metadata;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

import java.util.Set;

public class IndexParquetField extends ParquetField {

    @Override
    protected void addToGroup(MappedFieldType fieldType, ManagedVSR managedVSR, Object parseValue) {
        throw new IllegalStateException("Index field is not supposed to be added to group");
    }

    @Override
    public Set<FieldTypeCapabilities.Capability> supportedCapabilities() {
        return Set.of(
            FieldTypeCapabilities.Capability.BLOOM_FILTER,
            FieldTypeCapabilities.Capability.COLUMNAR_STORAGE,
            FieldTypeCapabilities.Capability.POINT_RANGE
        );
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

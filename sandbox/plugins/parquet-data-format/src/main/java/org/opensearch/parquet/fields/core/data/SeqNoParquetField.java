/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

import java.util.Arrays;
import java.util.List;

public class SeqNoParquetField extends ParquetField {

    @Override
    protected void addToGroup(MappedFieldType fieldType, ManagedVSR managedVSR, Object parseValue) {
        assert parseValue instanceof SeqNoFieldMapper.SequenceIdentifiers;

        final StructVector structVector = (StructVector) managedVSR.getVector(fieldType.name());
        final BigIntVector seqNoVector = structVector.addOrGet(
            SeqNoFieldMapper.NAME,
            FieldType.notNullable(new ArrowType.Int(64, true)),
            BigIntVector.class
        );
        final BigIntVector primaryTermVector = structVector.addOrGet(
            SeqNoFieldMapper.PRIMARY_TERM_NAME,
            FieldType.notNullable(new ArrowType.Int(64, true)),
            BigIntVector.class
        );

        seqNoVector.setSafe(managedVSR.getRowCount(), ((SeqNoFieldMapper.SequenceIdentifiers) parseValue).sequenceNumber());
        primaryTermVector.setSafe(managedVSR.getRowCount(), ((SeqNoFieldMapper.SequenceIdentifiers) parseValue).primaryTerm());
        structVector.setIndexDefined(managedVSR.getRowCount());
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Struct();
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.notNullable(getArrowType());
    }

    public List<Field> children() {
        return Arrays.asList(
            new Field(SeqNoFieldMapper.NAME, FieldType.notNullable(new ArrowType.Int(64, true)), null),
            new Field(SeqNoFieldMapper.PRIMARY_TERM_NAME, FieldType.notNullable(new ArrowType.Int(64, true)), null)
        );
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.vsr.ManagedVSR;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class BooleanParquetFieldTests extends OpenSearchTestCase {

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testArrowType() {
        BooleanParquetField field = new BooleanParquetField();
        assertTrue(field.getArrowType() instanceof ArrowType.Bool);
        assertTrue(field.getFieldType().isNullable());
    }

    public void testAddToGroupTrue() {
        BooleanParquetField field = new BooleanParquetField();
        MappedFieldType ft = new BooleanFieldMapper.BooleanFieldType("val");
        ManagedVSR vsr = createVSR(field);
        field.createField(ft, vsr, true);
        vsr.setRowCount(1);
        assertEquals(1, ((BitVector) vsr.getVector("val")).get(0));
        cleanupVSR(vsr);
    }

    public void testAddToGroupFalse() {
        BooleanParquetField field = new BooleanParquetField();
        MappedFieldType ft = new BooleanFieldMapper.BooleanFieldType("val");
        ManagedVSR vsr = createVSR(field);
        field.createField(ft, vsr, false);
        vsr.setRowCount(1);
        assertEquals(0, ((BitVector) vsr.getVector("val")).get(0));
        cleanupVSR(vsr);
    }

    private ManagedVSR createVSR(BooleanParquetField field) {
        Schema schema = new Schema(List.of(new Field("val", field.getFieldType(), null)));
        BufferAllocator child = allocator.newChildAllocator("bool-test", 0, Long.MAX_VALUE);
        return new ManagedVSR("bool-test", schema, child);
    }

    private void cleanupVSR(ManagedVSR vsr) {
        vsr.moveToFrozen();
        vsr.close();
    }
}

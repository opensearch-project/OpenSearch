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
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.vsr.ManagedVSR;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class BinaryParquetFieldTests extends OpenSearchTestCase {

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
        BinaryParquetField field = new BinaryParquetField();
        assertTrue(field.getArrowType() instanceof ArrowType.Binary);
        assertTrue(field.getFieldType().isNullable());
    }

    public void testAddToGroup() {
        BinaryParquetField field = new BinaryParquetField();
        MappedFieldType ft = new BinaryFieldMapper.BinaryFieldType("val");
        Schema schema = new Schema(List.of(new Field("val", field.getFieldType(), null)));
        BufferAllocator child = allocator.newChildAllocator("bin-test", 0, Long.MAX_VALUE);
        ManagedVSR vsr = new ManagedVSR("bin-test", schema, child);

        // BinaryParquetField uses set() not setSafe(), so allocate capacity first
        ((VarBinaryVector) vsr.getVector("val")).allocateNew(64, 1);

        byte[] data = new byte[] { 1, 2, 3, 4 };
        field.createField(ft, vsr, data);
        vsr.setRowCount(1);

        byte[] result = ((VarBinaryVector) vsr.getVector("val")).get(0);
        assertArrayEquals(data, result);

        vsr.moveToFrozen();
        vsr.close();
    }
}

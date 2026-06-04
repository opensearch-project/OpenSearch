/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.text;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TextParquetFieldTests extends OpenSearchTestCase {

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

    public void testTextFieldArrowType() {
        assertTrue(new TextParquetField().getArrowType() instanceof ArrowType.Utf8);
        assertTrue(new TextParquetField().getFieldType().isNullable());
    }

    public void testTextFieldAddToGroup() {
        TextParquetField field = new TextParquetField();
        MappedFieldType ft = new TextFieldMapper.TextFieldType("val");
        ManagedVSR vsr = createVSR("text-test", field, "val");
        field.createField(ft, vsr, "hello world");
        vsr.setRowCount(1);
        byte[] bytes = ((VarCharVector) vsr.getVector("val")).get(0);
        assertEquals("hello world", new String(bytes, StandardCharsets.UTF_8));
        cleanupVSR(vsr);
    }

    public void testKeywordFieldArrowType() {
        assertTrue(new KeywordParquetField().getArrowType() instanceof ArrowType.Utf8);
    }

    public void testKeywordFieldAddToGroup() {
        KeywordParquetField field = new KeywordParquetField();
        MappedFieldType ft = new KeywordFieldMapper.KeywordFieldType("val");
        ManagedVSR vsr = createVSR("kw-test", field, "val");
        field.createField(ft, vsr, "my_keyword");
        vsr.setRowCount(1);
        byte[] bytes = ((VarCharVector) vsr.getVector("val")).get(0);
        assertEquals("my_keyword", new String(bytes, StandardCharsets.UTF_8));
        cleanupVSR(vsr);
    }

    public void testIpFieldArrowType() {
        assertTrue(new IpParquetField().getArrowType() instanceof ArrowType.Binary);
    }

    public void testIpFieldAddToGroup() throws Exception {
        IpParquetField field = new IpParquetField();
        MappedFieldType ft = new IpFieldMapper.IpFieldType("val");
        ManagedVSR vsr = createVSR("ip-test", field, "val");
        InetAddress addr = InetAddress.getByName("127.0.0.1");
        field.createField(ft, vsr, addr);
        vsr.setRowCount(1);
        byte[] bytes = ((VarBinaryVector) vsr.getVector("val")).get(0);
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);
        cleanupVSR(vsr);
    }

    private ManagedVSR createVSR(String id, ParquetField pf, String fieldName) {
        Schema schema = new Schema(List.of(new Field(fieldName, pf.getFieldType(), null)));
        BufferAllocator child = allocator.newChildAllocator(id, 0, Long.MAX_VALUE);
        return new ManagedVSR(id, schema, child);
    }

    private void cleanupVSR(ManagedVSR vsr) {
        vsr.moveToFrozen();
        vsr.close();
    }
}

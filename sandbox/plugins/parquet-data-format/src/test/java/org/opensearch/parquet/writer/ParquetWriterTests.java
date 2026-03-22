/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.common.settings.Settings;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.test.OpenSearchTestCase;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ParquetWriterTests extends OpenSearchTestCase {

    private ArrowBufferPool bufferPool;
    private MappedFieldType idField;
    private MappedFieldType nameField;
    private MappedFieldType scoreField;
    private Schema schema;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        bufferPool = new ArrowBufferPool(Settings.EMPTY);
        idField = new NumberFieldMapper.NumberFieldType("id", NumberFieldMapper.NumberType.INTEGER);
        nameField = new KeywordFieldMapper.KeywordFieldType("name");
        scoreField = new NumberFieldMapper.NumberFieldType("score", NumberFieldMapper.NumberType.LONG);
        schema = buildSchema(List.of(idField, nameField, scoreField));
    }

    @Override
    public void tearDown() throws Exception {
        bufferPool.close();
        super.tearDown();
    }

    public void testAddDocReturnsSuccess() throws Exception {
        String filePath = createTempDir().resolve("success.parquet").toString();
        ParquetWriter writer = new ParquetWriter(filePath, 1L, new ParquetDataFormat(), schema, bufferPool, Settings.EMPTY);

        ParquetDocumentInput doc = new ParquetDocumentInput();
        doc.addField(idField, 1);
        doc.addField(nameField, "alice");
        doc.addField(scoreField, 100L);
        WriteResult result = writer.addDoc(doc);
        assertTrue(result instanceof WriteResult.Success);
        doc.close();
        writer.flush();
    }

    public void testSingleDocumentFlush() throws Exception {
        String filePath = createTempDir().resolve("single.parquet").toString();
        ParquetWriter writer = new ParquetWriter(filePath, 1L, new ParquetDataFormat(), schema, bufferPool, Settings.EMPTY);

        ParquetDocumentInput doc = new ParquetDocumentInput();
        doc.addField(idField, 42);
        doc.addField(nameField, "bob");
        doc.addField(scoreField, 500L);
        writer.addDoc(doc);
        doc.close();

        writer.flush();
        assertEquals(1, RustBridge.getFileMetadata(filePath).numRows());
    }

    public void testMultipleDocumentsFlush() throws Exception {
        String filePath = createTempDir().resolve("multi.parquet").toString();
        ParquetWriter writer = new ParquetWriter(filePath, 1L, new ParquetDataFormat(), schema, bufferPool, Settings.EMPTY);

        for (int i = 0; i < 10; i++) {
            ParquetDocumentInput doc = new ParquetDocumentInput();
            doc.addField(idField, i);
            doc.addField(nameField, "user_" + i);
            doc.addField(scoreField, (long) (i * 100));
            writer.addDoc(doc);
            doc.close();
        }

        FileInfos fileInfos = writer.flush();
        assertNotNull(fileInfos);
        assertTrue(new File(filePath).exists());
        assertEquals(10, RustBridge.getFileMetadata(filePath).numRows());
    }

    public void testFlushWithNoDocuments() throws Exception {
        String filePath = createTempDir().resolve("empty.parquet").toString();
        ParquetWriter writer = new ParquetWriter(filePath, 1L, new ParquetDataFormat(), schema, bufferPool, Settings.EMPTY);
        assertEquals(FileInfos.empty(), writer.flush());
    }

    public void testSyncAfterFlush() throws Exception {
        String filePath = createTempDir().resolve("sync.parquet").toString();
        ParquetWriter writer = new ParquetWriter(filePath, 1L, new ParquetDataFormat(), schema, bufferPool, Settings.EMPTY);

        ParquetDocumentInput doc = new ParquetDocumentInput();
        doc.addField(idField, 1);
        doc.addField(nameField, "alice");
        doc.addField(scoreField, 100L);
        writer.addDoc(doc);
        doc.close();

        writer.flush();
        writer.sync();
        assertTrue(new File(filePath).exists());
    }

    private Schema buildSchema(List<MappedFieldType> fieldTypes) {
        List<Field> fields = new ArrayList<>();
        for (MappedFieldType ft : fieldTypes) {
            ParquetField pf = ArrowFieldRegistry.getParquetField(ft.typeName());
            assertNotNull("No ParquetField registered for type: " + ft.typeName(), pf);
            fields.add(new Field(ft.name(), pf.getFieldType(), null));
        }
        return new Schema(fields);
    }
}

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
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.NativeObjectStore;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class ParquetWriterTests extends OpenSearchTestCase {

    private ArrowBufferPool bufferPool;
    private MappedFieldType idField;
    private MappedFieldType nameField;
    private MappedFieldType scoreField;
    private Schema schema;
    private ThreadPool threadPool;
    private NativeObjectStore objectStore;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        bufferPool = new ArrowBufferPool(Settings.EMPTY);
        idField = new NumberFieldMapper.NumberFieldType("id", NumberFieldMapper.NumberType.INTEGER);
        nameField = new KeywordFieldMapper.KeywordFieldType("name");
        scoreField = new NumberFieldMapper.NumberFieldType("score", NumberFieldMapper.NumberType.LONG);
        schema = buildSchema(List.of(idField, nameField, scoreField));
        Settings settings = Settings.builder().put("node.name", "parquetwriter-test").build();
        threadPool = new ThreadPool(
            settings,
            new FixedExecutorBuilder(
                settings,
                ParquetDataFormatPlugin.PARQUET_THREAD_POOL_NAME,
                1,
                -1,
                "thread_pool." + ParquetDataFormatPlugin.PARQUET_THREAD_POOL_NAME
            )
        );
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        if (objectStore != null) {
            objectStore.close();
        }
        bufferPool.close();
        super.tearDown();
    }

    public void testAddDocReturnsSuccess() throws Exception {
        Path dir = createTempDir();
        objectStore = new NativeObjectStore("local", "{\"root\": \"" + dir + "\"}");
        ParquetWriter writer = createParquetWriter(objectStore, "success.parquet");

        ParquetDocumentInput doc = new ParquetDocumentInput();
        doc.addField(idField, 1);
        doc.addField(nameField, "alice");
        doc.addField(scoreField, 100L);
        WriteResult result = writer.addDoc(doc);
        assertTrue(result instanceof WriteResult.Success);
        doc.close();
        writer.flush();
        writer.close();
    }

    public void testSingleDocumentFlush() throws Exception {
        Path dir = createTempDir();
        objectStore = new NativeObjectStore("local", "{\"root\": \"" + dir + "\"}");
        ParquetWriter writer = createParquetWriter(objectStore, "single.parquet");

        ParquetDocumentInput doc = new ParquetDocumentInput();
        doc.addField(idField, 42);
        doc.addField(nameField, "bob");
        doc.addField(scoreField, 500L);
        writer.addDoc(doc);
        doc.close();

        writer.flush();
        writer.close();
        assertEquals(1, RustBridge.getFileMetadata(dir.resolve("single.parquet").toString()).numRows());
    }

    public void testMultipleDocumentsFlush() throws Exception {
        Path dir = createTempDir();
        objectStore = new NativeObjectStore("local", "{\"root\": \"" + dir + "\"}");
        ParquetWriter writer = createParquetWriter(objectStore, "multi.parquet");

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
        writer.close();
        assertTrue(Files.exists(dir.resolve("multi.parquet")));
        assertEquals(10, RustBridge.getFileMetadata(dir.resolve("multi.parquet").toString()).numRows());
    }

    public void testFlushWithNoDocuments() throws Exception {
        Path dir = createTempDir();
        objectStore = new NativeObjectStore("local", "{\"root\": \"" + dir + "\"}");
        ParquetWriter writer = createParquetWriter(objectStore, "empty.parquet");
        assertEquals(FileInfos.empty(), writer.flush());
        writer.close();
    }

    public void testSyncAfterFlush() throws Exception {
        Path dir = createTempDir();
        objectStore = new NativeObjectStore("local", "{\"root\": \"" + dir + "\"}");
        ParquetWriter writer = createParquetWriter(objectStore, "sync.parquet");

        ParquetDocumentInput doc = new ParquetDocumentInput();
        doc.addField(idField, 1);
        doc.addField(nameField, "alice");
        doc.addField(scoreField, 100L);
        writer.addDoc(doc);
        doc.close();

        writer.flush();
        writer.sync();
        writer.close();
        assertTrue(Files.exists(dir.resolve("sync.parquet")));
    }

    private ParquetWriter createParquetWriter(NativeObjectStore store, String path) {
        return new ParquetWriter(store, path, 1L, new ParquetDataFormat(), schema, bufferPool, Settings.EMPTY, threadPool);
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

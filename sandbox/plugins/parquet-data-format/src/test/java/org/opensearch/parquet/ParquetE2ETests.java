/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.engine.ParquetIndexingEngine;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.parquet.writer.ParquetWriter;
import org.opensearch.test.OpenSearchTestCase;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * E2E tests for the Parquet write pipeline, covering both direct ParquetWriter
 * usage and the ParquetIndexingEngine entry point.
 */
public class ParquetE2ETests extends OpenSearchTestCase {

    private ArrowBufferPool bufferPool;
    private MappedFieldType ageField;
    private MappedFieldType nameField;
    private MappedFieldType scoreField;
    private Schema schema;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        bufferPool = new ArrowBufferPool();
        ageField = new NumberFieldMapper.NumberFieldType("age", NumberFieldMapper.NumberType.INTEGER);
        nameField = new KeywordFieldMapper.KeywordFieldType("name");
        scoreField = new NumberFieldMapper.NumberFieldType("score", NumberFieldMapper.NumberType.LONG);
        schema = buildSchema(Arrays.asList(ageField, nameField, scoreField));
    }

    @Override
    public void tearDown() throws Exception {
        bufferPool.close();
        super.tearDown();
    }

    // --- ParquetWriter tests ---

    public void testWriterMultipleDocumentsAndFlush() throws Exception {
        String filePath = createTempDir().resolve("docs.parquet").toString();
        ParquetWriter writer = new ParquetWriter(filePath, 1L, new ParquetDataFormat(), schema, bufferPool);

        for (int i = 0; i < 10; i++) {
            ParquetDocumentInput doc = new ParquetDocumentInput();
            doc.addField(ageField, 20 + i);
            doc.addField(nameField, "user_" + i);
            doc.addField(scoreField, (long) (i * 100));
            WriteResult result = writer.addDoc(doc);
            assertTrue(result instanceof WriteResult.Success);
            doc.close();
        }

        FileInfos fileInfos = writer.flush();
        assertNotNull(fileInfos);
        assertTrue("Parquet file should exist", new File(filePath).exists());
        assertEquals(10, RustBridge.getFileMetadata(filePath).numRows());
    }

    public void testWriterSingleDocument() throws Exception {
        String filePath = createTempDir().resolve("single.parquet").toString();
        MappedFieldType idField = new NumberFieldMapper.NumberFieldType("id", NumberFieldMapper.NumberType.INTEGER);
        ParquetWriter writer = new ParquetWriter(filePath, 1L, new ParquetDataFormat(),
            buildSchema(Collections.singletonList(idField)), bufferPool);

        ParquetDocumentInput doc = new ParquetDocumentInput();
        doc.addField(idField, 42);
        writer.addDoc(doc);
        doc.close();

        writer.flush();
        assertEquals(1, RustBridge.getFileMetadata(filePath).numRows());
    }

    public void testWriterFlushWithNoDocuments() throws Exception {
        String filePath = createTempDir().resolve("empty.parquet").toString();
        ParquetWriter writer = new ParquetWriter(filePath, 1L, new ParquetDataFormat(), schema, bufferPool);

        assertEquals(FileInfos.empty(), writer.flush());
    }

    // --- ParquetIndexingEngine tests ---

    public void testEngineWriteDocumentsAndFlush() throws Exception {
        Path tempDir = createTempDir();
        ParquetIndexingEngine engine = createEngine(tempDir);
        Writer<ParquetDocumentInput> writer = engine.createWriter(1L);

        for (int i = 0; i < 10; i++) {
            ParquetDocumentInput doc = engine.newDocumentInput();
            doc.addField(ageField, 20 + i);
            doc.addField(nameField, "user_" + i);
            doc.addField(scoreField, (long) (i * 100));
            writer.addDoc(doc);
            doc.close();
        }

        writer.flush();

        String expectedFile = getExpectedParquetPath(tempDir, 1L);
        assertTrue("Parquet file should exist", Files.exists(Path.of(expectedFile)));
        assertEquals(10, RustBridge.getFileMetadata(expectedFile).numRows());
    }

    public void testEngineMultipleWriterGenerations() throws Exception {
        Path tempDir = createTempDir();
        ParquetIndexingEngine engine = createEngine(tempDir);

        for (long gen = 1; gen <= 2; gen++) {
            Writer<ParquetDocumentInput> writer = engine.createWriter(gen);
            ParquetDocumentInput doc = engine.newDocumentInput();
            doc.addField(ageField, (int) (20 + gen));
            doc.addField(nameField, "user_" + gen);
            doc.addField(scoreField, gen * 100);
            writer.addDoc(doc);
            doc.close();
            writer.flush();
        }

        assertEquals(1, RustBridge.getFileMetadata(getExpectedParquetPath(tempDir, 1L)).numRows());
        assertEquals(1, RustBridge.getFileMetadata(getExpectedParquetPath(tempDir, 2L)).numRows());
    }

    // --- Helpers ---

    private ParquetIndexingEngine createEngine(Path tempDir) throws java.io.IOException {
        String indexUUID = "test_index_uuid";
        ShardId shardId = new ShardId("test_index", indexUUID, 0);
        Path dataPath = tempDir.resolve(indexUUID).resolve("0");
        // In production, the shard path and plugin subdirectory would already exist
        Files.createDirectories(dataPath.resolve("parquet"));
        ShardPath shardPath = new ShardPath(false, dataPath, dataPath, shardId);
        return new ParquetIndexingEngine(new ParquetDataFormat(), shardPath, () -> schema);
    }

    private String getExpectedParquetPath(Path tempDir, long generation) {
        return tempDir.resolve("test_index_uuid").resolve("0").resolve("parquet")
            .resolve(ParquetIndexingEngine.FILE_NAME_PREFIX + "_" + generation + ParquetIndexingEngine.FILE_NAME_EXT)
            .toString();
    }

    private Schema buildSchema(List<MappedFieldType> fieldTypes) {
        List<Field> fields = new java.util.ArrayList<>();
        for (MappedFieldType ft : fieldTypes) {
            ParquetField pf = ArrowFieldRegistry.getParquetField(ft.typeName());
            assertNotNull("No ParquetField registered for type: " + ft.typeName(), pf);
            fields.add(new Field(ft.name(), pf.getFieldType(), null));
        }
        return new Schema(fields);
    }
}

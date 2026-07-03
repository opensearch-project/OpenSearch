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
import org.opensearch.Version;
import org.opensearch.arrow.allocator.ArrowNativeAllocator;
import org.opensearch.arrow.spi.NativeAllocatorPoolConfig;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.ParquetBaseTests;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.encryption.PmeFileEncryptionInputs;
import org.opensearch.parquet.encryption.PmeKeyDerivation;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ParquetWriterTests extends ParquetBaseTests {

    private final ParquetDataFormat parquetFormat = new ParquetDataFormat();
    private ArrowNativeAllocator nativeAllocator;
    private ArrowBufferPool bufferPool;
    private MappedFieldType idField;
    private MappedFieldType nameField;
    private MappedFieldType scoreField;
    private Schema schema;
    private ThreadPool threadPool;
    private IndexSettings indexSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        nativeAllocator = new ArrowNativeAllocator();
        nativeAllocator.getOrCreatePool(NativeAllocatorPoolConfig.POOL_INGEST, 0L, Long.MAX_VALUE, null);
        bufferPool = new ArrowBufferPool(Settings.EMPTY, nativeAllocator);
        idField = new NumberFieldMapper.NumberFieldType("id", NumberFieldMapper.NumberType.INTEGER);
        nameField = new KeywordFieldMapper.KeywordFieldType("name");
        scoreField = new NumberFieldMapper.NumberFieldType("score", NumberFieldMapper.NumberType.LONG);
        assignTestCapabilities(idField, parquetFormat);
        assignTestCapabilities(nameField, parquetFormat);
        assignTestCapabilities(scoreField, parquetFormat);
        schema = buildSchema(List.of(idField, nameField, scoreField));
        Settings indexSettingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(indexSettingsBuilder).build();
        indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
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
        bufferPool.close();
        if (nativeAllocator != null) {
            nativeAllocator.close();
            nativeAllocator = null;
        }
        super.tearDown();
    }

    public void testAddDocReturnsSuccess() throws Exception {
        String filePath = createTempDir().resolve("success.parquet").toString();
        try (ParquetWriter writer = new ParquetWriter(
            filePath, 1L, 1L, new ParquetDataFormat(), schema, () -> schema, bufferPool, indexSettings, threadPool, null
        )) {
            ParquetDocumentInput doc = new ParquetDocumentInput();
            populateMetadataFields(doc);
            doc.addField(idField, 1);
            doc.addField(nameField, "alice");
            doc.addField(scoreField, 100L);
            doc.setRowId(DocumentInput.ROW_ID_FIELD, 0);
            WriteResult result = writer.addDoc(doc);
            assertTrue(result instanceof WriteResult.Success);
            doc.close();
            writer.flush(FlushInput.EMPTY);
        }
    }

    public void testSingleDocumentFlush() throws Exception {
        String filePath = createTempDir().resolve("single.parquet").toString();
        try (ParquetWriter writer = new ParquetWriter(
            filePath, 1L, 1L, new ParquetDataFormat(), schema, () -> schema, bufferPool, indexSettings, threadPool, null
        )) {
            ParquetDocumentInput doc = new ParquetDocumentInput();
            populateMetadataFields(doc);
            doc.addField(idField, 42);
            doc.addField(nameField, "bob");
            doc.addField(scoreField, 500L);
            doc.setRowId(DocumentInput.ROW_ID_FIELD, 0);
            writer.addDoc(doc);
            doc.close();

            writer.flush(FlushInput.EMPTY);
            assertEquals(1, RustBridge.getFileMetadata(filePath).numRows());
        }
    }

    public void testMultipleDocumentsFlush() throws Exception {
        String filePath = createTempDir().resolve("multi.parquet").toString();
        try (ParquetWriter writer = new ParquetWriter(
            filePath, 1L, 1L, new ParquetDataFormat(), schema, () -> schema, bufferPool, indexSettings, threadPool, null
        )) {
            for (int i = 0; i < 10; i++) {
                ParquetDocumentInput doc = new ParquetDocumentInput();
                populateMetadataFields(doc);
                doc.addField(idField, i);
                doc.addField(nameField, "user_" + i);
                doc.addField(scoreField, (long) (i * 100));
                doc.setRowId("__row_id__", i);
                writer.addDoc(doc);
                doc.close();
            }

            FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
            assertNotNull(fileInfos);
            assertTrue(Files.exists(Path.of(filePath)));
            assertEquals(10, RustBridge.getFileMetadata(filePath).numRows());
        }
    }

    public void testFlushWithNoDocuments() throws Exception {
        String filePath = createTempDir().resolve("empty.parquet").toString();
        try (ParquetWriter writer = new ParquetWriter(
            filePath, 1L, 1L, new ParquetDataFormat(), schema, () -> schema, bufferPool, indexSettings, threadPool, null
        )) {
            assertEquals(FileInfos.empty(), writer.flush(FlushInput.EMPTY));
        }
    }

    public void testSyncAfterFlush() throws Exception {
        String filePath = createTempDir().resolve("sync.parquet").toString();
        try (ParquetWriter writer = new ParquetWriter(
            filePath, 1L, 1L, new ParquetDataFormat(), schema, () -> schema, bufferPool, indexSettings, threadPool, null
        )) {
            ParquetDocumentInput doc = new ParquetDocumentInput();
            populateMetadataFields(doc);
            doc.addField(idField, 1);
            doc.addField(nameField, "alice");
            doc.addField(scoreField, 100L);
            doc.setRowId("__row_id__", 0);
            writer.addDoc(doc);
            doc.close();

            writer.flush(FlushInput.EMPTY);
            assertTrue(Files.exists(Path.of(filePath)));
        }
    }

    public void testFlushPersistsPerFilePmeMetadata() throws Exception {
        String filePath = createTempDir().resolve("encrypted.parquet").toString();

        // Build encryption inputs directly from known key material.
        byte[] dataKey = randomByteArrayOfLength(32);
        byte[] messageId = randomByteArrayOfLength(16);
        byte[] footerKey = PmeKeyDerivation.deriveFooterKey(dataKey, messageId);
        byte[] aadPrefix = PmeKeyDerivation.buildAadPrefix(messageId);
        PmeFileEncryptionInputs encryptionConfig = PmeFileEncryptionInputs.forDecryption(footerKey, aadPrefix);

        FileInfos fileInfos;
        try (ParquetWriter writer = new ParquetWriter(
            filePath, 1L, 1L, new ParquetDataFormat(), schema, () -> schema, bufferPool, indexSettings, threadPool, null, encryptionConfig
        )) {
            ParquetDocumentInput doc = new ParquetDocumentInput();
            populateMetadataFields(doc);
            doc.addField(idField, 7);
            doc.addField(nameField, "enc");
            doc.addField(scoreField, 700L);
            doc.setRowId("__row_id__", 0);
            writer.addDoc(doc);
            doc.close();

            fileInfos = writer.flush(FlushInput.EMPTY);
        }
        String fileName = Path.of(filePath).getFileName().toString();
        Map<String, String> metadata = fileInfos.writerFilesMap().values().iterator().next().metadataForFile(fileName);

        // PME key_metadata is now stored only in the Parquet file footer — not in catalog metadata.
        // The WriterFileSet must be empty to avoid duplicating key material in the catalog.
        assertTrue(
            "Encrypted Parquet file must not emit PME per-file catalog metadata; got: " + metadata,
            metadata.isEmpty()
        );
    }

    private Schema buildSchema(List<MappedFieldType> fieldTypes) {
        List<Field> fields = new ArrayList<>();
        for (MappedFieldType ft : fieldTypes) {
            ParquetField pf = ArrowFieldRegistry.getParquetField(ft.typeName());
            assertNotNull("No ParquetField registered for type: " + ft.typeName(), pf);
            fields.add(new Field(ft.name(), pf.getFieldType(), null));
        }
        fields.addAll(metadataFields());
        return new Schema(fields);
    }
}

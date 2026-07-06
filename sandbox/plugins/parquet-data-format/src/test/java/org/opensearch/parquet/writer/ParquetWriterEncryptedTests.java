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
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.encryption.PmeDataKey;
import org.opensearch.parquet.encryption.PmeFileEncryptionInputs;
import org.opensearch.parquet.encryption.PmeFileKeyMetadata;
import org.opensearch.parquet.encryption.PmeKeyDerivation;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.stats.ParquetShardStatsTracker;
import org.opensearch.parquet.ParquetBaseTests;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Encrypted variants of {@link ParquetWriterTests}.
 */
public class ParquetWriterEncryptedTests extends ParquetBaseTests {

    private ArrowNativeAllocator nativeAllocator;
    private ArrowBufferPool bufferPool;
    private MappedFieldType idField;
    private MappedFieldType nameField;
    private MappedFieldType scoreField;
    private Schema schema;
    private ThreadPool threadPool;
    private IndexSettings indexSettings;

    private byte[] dataKey;

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
        schema = buildSchema(List.of(idField, nameField, scoreField));
        Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(idxSettings).build();
        indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        Settings settings = Settings.builder().put("node.name", "parquetwriter-encrypted-test").build();
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
        dataKey = randomByteArrayOfLength(PmeKeyDerivation.DATA_KEY_BYTES);
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

    private PmeFileEncryptionInputs encryptionInputs() {
        return PmeFileEncryptionInputs.create(new PmeDataKey(dataKey));
    }

    private PmeFileEncryptionInputs decryptionInputs(String filePath) throws Exception {
        byte[] keyMetadataBytes = RustBridge.readKeyMetadata(filePath);
        assertNotNull("Encrypted file must contain key_metadata", keyMetadataBytes);
        PmeFileKeyMetadata meta = PmeFileKeyMetadata.parse(keyMetadataBytes);
        assertEquals(PmeFileKeyMetadata.DEFAULT_DATA_KEY_ID, meta.dataKeyId());
        byte[] footerKey = PmeKeyDerivation.deriveFooterKey(dataKey, meta.messageId());
        byte[] aadPrefix = PmeKeyDerivation.buildAadPrefix(meta.messageId());
        return PmeFileEncryptionInputs.forDecryption(footerKey, aadPrefix);
    }

    private Schema buildSchema(List<MappedFieldType> fieldTypes) {
        List<Field> fields = new ArrayList<>();
        for (MappedFieldType ft : fieldTypes) {
            ParquetField pf = ArrowFieldRegistry.getParquetField(ft.typeName());
            assertNotNull("No ParquetField registered for type: " + ft.typeName(), pf);
            fields.add(new Field(ft.name(), pf.getFieldType(), null));
        }
        // Metadata fields are required by the ParquetDocumentInput assertion
        fields.addAll(metadataFields());
        return new Schema(fields);
    }

    private ParquetWriter createWriter(String filePath, PmeFileEncryptionInputs enc) {
        return new ParquetWriter(
            filePath,
            1L,
            1L,
            new ParquetDataFormat(),
            schema,
            () -> schema,
            bufferPool,
            indexSettings,
            threadPool,
            null,
            (ParquetShardStatsTracker) null,
            enc
        );
    }

    public void testEncryptedSingleDocumentFlush() throws Exception {
        String filePath = createTempDir().resolve("encrypted-single.parquet").toString();
        ParquetWriter writer = createWriter(filePath, encryptionInputs());

        ParquetDocumentInput doc = new ParquetDocumentInput();
        populateMetadataFields(doc);
        doc.addField(idField, 42);
        doc.addField(nameField, "alice");
        doc.addField(scoreField, 500L);
        doc.setRowId("__row_id__", 0);
        writer.addDoc(doc);
        doc.close();
        writer.flush(FlushInput.EMPTY);

        assertTrue("Encrypted parquet file must exist", Files.exists(Path.of(filePath)));
        expectThrows(Exception.class, () -> RustBridge.getFileMetadata(filePath));
        PmeFileEncryptionInputs dec = decryptionInputs(filePath);
        assertEquals(1, RustBridge.getFileMetadata(filePath, dec).numRows());
    }

    public void testEncryptedMultipleDocumentsFlush() throws Exception {
        String filePath = createTempDir().resolve("encrypted-multi.parquet").toString();
        ParquetWriter writer = createWriter(filePath, encryptionInputs());

        int count = randomIntBetween(2, 20);
        for (int i = 0; i < count; i++) {
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
        expectThrows(Exception.class, () -> RustBridge.getFileMetadata(filePath));
        PmeFileEncryptionInputs dec = decryptionInputs(filePath);
        assertEquals(count, RustBridge.getFileMetadata(filePath, dec).numRows());
    }

    public void testEncryptedReadWithWrongKeyFails() throws Exception {
        String filePath = createTempDir().resolve("encrypted-wrong-key.parquet").toString();
        ParquetWriter writer = createWriter(filePath, encryptionInputs());

        ParquetDocumentInput doc = new ParquetDocumentInput();
        populateMetadataFields(doc);
        doc.addField(idField, 1);
        doc.addField(nameField, "bob");
        doc.addField(scoreField, 99L);
        doc.setRowId("__row_id__", 0);
        writer.addDoc(doc);
        doc.close();
        writer.flush(FlushInput.EMPTY);

        byte[] keyMetadataBytes = RustBridge.readKeyMetadata(filePath);
        PmeFileKeyMetadata meta = PmeFileKeyMetadata.parse(keyMetadataBytes);
        byte[] wrongDataKey = randomByteArrayOfLength(PmeKeyDerivation.DATA_KEY_BYTES);
        byte[] wrongFooterKey = PmeKeyDerivation.deriveFooterKey(wrongDataKey, meta.messageId());
        byte[] aadPrefix = PmeKeyDerivation.buildAadPrefix(meta.messageId());
        PmeFileEncryptionInputs wrongDec = PmeFileEncryptionInputs.forDecryption(wrongFooterKey, aadPrefix);
        expectThrows(Exception.class, () -> RustBridge.getDecryptedNumRows(filePath, wrongDec));
    }

    public void testEncryptedFooterKeyMetadataRoundTrip() throws Exception {
        String filePath = createTempDir().resolve("encrypted-meta.parquet").toString();
        ParquetWriter writer = createWriter(filePath, encryptionInputs());

        ParquetDocumentInput doc = new ParquetDocumentInput();
        populateMetadataFields(doc);
        doc.addField(idField, 7);
        doc.addField(nameField, "meta-test");
        doc.addField(scoreField, 777L);
        doc.setRowId("__row_id__", 0);
        writer.addDoc(doc);
        doc.close();
        writer.flush(FlushInput.EMPTY);

        byte[] keyMetadataBytes = RustBridge.readKeyMetadata(filePath);
        assertNotNull(keyMetadataBytes);

        PmeFileKeyMetadata meta = PmeFileKeyMetadata.parse(keyMetadataBytes);
        assertEquals(1, meta.version());
        assertEquals(PmeFileKeyMetadata.DEFAULT_DATA_KEY_ID, meta.dataKeyId());
        assertEquals(16, meta.messageId().length);
    }
}


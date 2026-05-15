/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.engine;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.WriterConfig;
import org.opensearch.index.engine.exec.PrimaryTermFieldType;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ArrowSchemaBuilder;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.parquet.engine.ParquetDataFormatAwareEngineTests.ID_FIELD;
import static org.opensearch.parquet.engine.ParquetDataFormatAwareEngineTests.SEQ_NO_FIELD;
import static org.opensearch.parquet.engine.ParquetDataFormatAwareEngineTests.VERSION_FIELD;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ParquetIndexingEngineTests extends OpenSearchTestCase {

    private MappedFieldType idField;
    private MappedFieldType nameField;
    private MappedFieldType scoreField;
    private Schema schema;
    private Path tempDir;
    private ParquetIndexingEngine engine;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        idField = new NumberFieldMapper.NumberFieldType("id", NumberFieldMapper.NumberType.INTEGER);
        nameField = new KeywordFieldMapper.KeywordFieldType("name");
        scoreField = new NumberFieldMapper.NumberFieldType("score", NumberFieldMapper.NumberType.LONG);
        schema = buildSchema(List.of(idField, nameField, scoreField));
        tempDir = createTempDir();
        Settings settings = Settings.builder().put("node.name", "parquetengine-test").build();
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
        engine = createEngine();
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    public void testCreateWriterAndFlush() throws Exception {
        Writer<ParquetDocumentInput> writer = engine.createWriter(new WriterConfig(1L));

        for (int i = 0; i < 5; i++) {
            ParquetDocumentInput doc = engine.newDocumentInput();
            populateMetadataFields(doc);
            doc.addField(idField, i);
            doc.addField(nameField, "user_" + i);
            doc.addField(scoreField, (long) (i * 100));
            doc.setRowId("__row_id__", i);
            writer.addDoc(doc);
            doc.close();
        }

        writer.flush();
        String expectedFile = getExpectedParquetPath(1L);
        assertTrue(Files.exists(Path.of(expectedFile)));
        assertEquals(5, RustBridge.getFileMetadata(expectedFile).numRows());
    }

    public void testMultipleWriterGenerations() throws Exception {
        for (long gen = 1; gen <= 3; gen++) {
            Writer<ParquetDocumentInput> writer = engine.createWriter(new WriterConfig(gen));
            ParquetDocumentInput doc = engine.newDocumentInput();
            populateMetadataFields(doc);
            doc.addField(idField, (int) gen);
            doc.addField(nameField, "user_" + gen);
            doc.addField(scoreField, gen * 100);
            doc.setRowId("__row_id__", gen);

            writer.addDoc(doc);
            doc.close();
            writer.flush();
            assertEquals(1, RustBridge.getFileMetadata(getExpectedParquetPath(gen)).numRows());
        }
    }

    public void testNewDocumentInput() {
        ParquetDocumentInput doc = engine.newDocumentInput();
        populateMetadataFields(doc);
        assertNotNull(doc);
        doc.setRowId("__row_id__", 0);
        assertEquals(4, doc.getFinalInput().size());
    }

    public void testGetDataFormat() {
        assertEquals("parquet", engine.getDataFormat().name());
    }

    public void testRefreshReturnsEmptyResult() throws Exception {
        assertNotNull(engine.refresh(RefreshInput.builder().build()));
        assertTrue(engine.refresh(RefreshInput.builder().build()).refreshedSegments().isEmpty());
    }

    public void testRefreshWithNullInput() throws Exception {
        RefreshResult result = engine.refresh(null);
        assertNotNull(result);
        assertTrue(result.refreshedSegments().isEmpty());
    }

    public void testGetMergerReturnsNonNull() {
        assertNotNull(engine.getMerger());
    }

    public void testGetNextWriterGenerationThrows() {
        expectThrows(UnsupportedOperationException.class, () -> engine.getNextWriterGeneration());
    }

    public void testDeleteFiles() throws Exception {
        Path parquetFile = tempDir.resolve("test.parquet");
        Files.createFile(parquetFile);
        assertTrue(Files.exists(parquetFile));

        engine.deleteFiles(Map.of("parquet", List.of(parquetFile.toString())));
        assertFalse(Files.exists(parquetFile));
    }

    public void testDeleteFilesIgnoresUnrelatedFormats() throws Exception {
        Path luceneFile = tempDir.resolve("test_lucene");
        Files.createFile(luceneFile);
        assertTrue(Files.exists(luceneFile));
        engine.deleteFiles(Map.of("lucene", List.of("test_lucene")));
        assertTrue(Files.exists(luceneFile));
    }

    public void testFlushWithNoDocumentsReturnsEmpty() throws Exception {
        Writer<ParquetDocumentInput> writer = engine.createWriter(new WriterConfig(1L));
        assertEquals(FileInfos.empty(), writer.flush());
    }

    private ParquetIndexingEngine createEngine() {
        try {
            String indexUUID = "test_index_uuid";
            ShardId shardId = new ShardId("test_index", indexUUID, 0);
            Path dataPath = tempDir.resolve(indexUUID).resolve("0");
            Files.createDirectories(dataPath.resolve("parquet"));
            ShardPath shardPath = new ShardPath(false, dataPath, dataPath, shardId);
            Settings indexSettingsBuilder = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build();
            IndexMetadata indexMetadata = IndexMetadata.builder("test_index").settings(indexSettingsBuilder).build();
            IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
            MapperService mapperService = createMockMapperService(schema, indexSettings);
            return new ParquetIndexingEngine(
                Settings.EMPTY,
                new ParquetDataFormat(),
                shardPath,
                () -> ArrowSchemaBuilder.getSchema(mapperService),
                () -> mapperService.getIndexSettings().getIndexMetadata().getMappingVersion(),
                indexSettings,
                threadPool
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getExpectedParquetPath(long generation) {
        return tempDir.resolve("test_index_uuid")
            .resolve("0")
            .resolve("parquet")
            .resolve(ParquetIndexingEngine.FILE_NAME_PREFIX + "_" + generation + ParquetIndexingEngine.FILE_NAME_EXT)
            .toString();
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

    private MapperService createMockMapperService(Schema schema, IndexSettings indexSettings) {
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.documentMapper()).thenReturn(null);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        return mapperService;
    }

    public static List<Field> metadataFields() {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(VersionFieldMapper.NAME, FieldType.notNullable(new ArrowType.Int(64, true)), null));
        fields.add(new Field(SeqNoFieldMapper.NAME, FieldType.notNullable(new ArrowType.Int(64, true)), null));
        fields.add(new Field(SeqNoFieldMapper.PRIMARY_TERM_NAME, FieldType.notNullable(new ArrowType.Int(64, true)), null));
        fields.add(new Field(IdFieldMapper.NAME, FieldType.notNullable(new ArrowType.Binary()), null));
        return fields;
    }

    public static void populateMetadataFields(ParquetDocumentInput input) {
        input.addField(SEQ_NO_FIELD, 100L);
        input.addField(ID_FIELD, "id".getBytes(StandardCharsets.UTF_8));
        input.addField(VERSION_FIELD, 1L);
        input.addField(PrimaryTermFieldType.INSTANCE, 1L);
    }
}

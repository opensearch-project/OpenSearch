/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.engine;

import org.apache.arrow.vector.types.pojo.Field;
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
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        Writer<ParquetDocumentInput> writer = engine.createWriter(1L);

        for (int i = 0; i < 5; i++) {
            ParquetDocumentInput doc = engine.newDocumentInput();
            doc.addField(idField, i);
            doc.addField(nameField, "user_" + i);
            doc.addField(scoreField, (long) (i * 100));
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
            Writer<ParquetDocumentInput> writer = engine.createWriter(gen);
            ParquetDocumentInput doc = engine.newDocumentInput();
            doc.addField(idField, (int) gen);
            doc.addField(nameField, "user_" + gen);
            doc.addField(scoreField, gen * 100);
            writer.addDoc(doc);
            doc.close();
            writer.flush();
            assertEquals(1, RustBridge.getFileMetadata(getExpectedParquetPath(gen)).numRows());
        }
    }

    public void testNewDocumentInput() {
        ParquetDocumentInput doc = engine.newDocumentInput();
        assertNotNull(doc);
        assertTrue(doc.getFinalInput().isEmpty());
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
        Writer<ParquetDocumentInput> writer = engine.createWriter(1L);
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
            return new ParquetIndexingEngine(Settings.EMPTY, new ParquetDataFormat(), shardPath, () -> schema, indexSettings, threadPool);
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
        return new Schema(fields);
    }
}

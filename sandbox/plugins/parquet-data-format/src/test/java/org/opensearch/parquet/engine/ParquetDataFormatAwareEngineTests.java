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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.AbstractDataFormatAwareEngineTestCase;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.stub.MockSearchBackEndPlugin;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.PrecomputedChecksumStrategy;
import org.opensearch.index.store.Store;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.fields.core.data.SeqNoParquetField;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs the {@link AbstractDataFormatAwareEngineTestCase} suite with the real
 * Parquet native engine. Validates that the DFAE orchestration layer works
 * correctly with the Parquet writer, native Rust bridge, and Arrow vectors.
 *
 * <p>Requires the native library ({@code libopensearch_native}) to be built
 * and available on the library path. The Gradle task {@code buildRustLibrary}
 * is a dependency of the test task in the Parquet plugin's {@code build.gradle}.
 *
 * <p>Provides a hardcoded Arrow schema with keyword and integer fields,
 * bypassing the need for a real {@link org.opensearch.index.mapper.MapperService}.
 * The {@link #createDocumentInput()} method returns a {@link ParquetDocumentInput}
 * pre-populated with placeholder metadata fields (version, seqNo) so that
 * {@link org.opensearch.index.engine.DataFormatAwareEngine#indexIntoEngine}
 * can call {@code updateField} on them.
 */
public class ParquetDataFormatAwareEngineTests extends AbstractDataFormatAwareEngineTestCase {

    private static final MappedFieldType NAME_FIELD = new KeywordFieldMapper.KeywordFieldType("name");
    private static final MappedFieldType AGE_FIELD = new NumberFieldMapper.NumberFieldType("age", NumberFieldMapper.NumberType.INTEGER);
    private static final MappedFieldType VERSION_FIELD = new NumberFieldMapper.NumberFieldType(
        VersionFieldMapper.NAME,
        NumberFieldMapper.NumberType.LONG
    ) {
        @Override
        public String typeName() {
            return VersionFieldMapper.CONTENT_TYPE;
        }
    };
    private static final MappedFieldType SEQNO_FIELD = new NumberFieldMapper.NumberFieldType(
        SeqNoFieldMapper.NAME,
        NumberFieldMapper.NumberType.LONG
    ) {
        @Override
        public String typeName() {
            return SeqNoFieldMapper.CONTENT_TYPE;
        }
    };

    private Schema schema;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        schema = buildSchema();
    }

    @Override
    protected Store createStore() throws IOException {
        // Parquet engine needs a real filesystem path for writing .parquet files
        Path dataPath = createTempDir().resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        java.nio.file.Files.createDirectories(dataPath);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .build()
        );
        ShardPath shardPath = new ShardPath(false, dataPath, dataPath, shardId);
        return new Store(
            shardId,
            indexSettings,
            new org.apache.lucene.store.NIOFSDirectory(dataPath),
            new org.opensearch.test.DummyShardLock(shardId),
            Store.OnClose.EMPTY,
            shardPath
        );
    }

    @Override
    protected ThreadPool buildThreadPool() {
        Settings settings = Settings.builder().put("node.name", "parquet-dfae-test").build();
        return new TestThreadPool(
            getClass().getName(),
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
    protected DataFormatPlugin createDataFormatPlugin() {
        // Create a plugin that bypasses MapperService and uses our hardcoded schema
        return new DataFormatPlugin() {
            private final ParquetDataFormat dataFormat = new ParquetDataFormat();

            @Override
            public org.opensearch.index.engine.dataformat.DataFormat getDataFormat() {
                return dataFormat;
            }

            @Override
            public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig engineConfig) {
                return new ParquetIndexingEngine(
                    Settings.EMPTY,
                    dataFormat,
                    engineConfig.store().shardPath(),
                    () -> schema,
                    engineConfig.indexSettings(),
                    threadPool,
                    new PrecomputedChecksumStrategy()
                );
            }
        };
    }

    @Override
    protected SearchBackEndPlugin<?> createSearchBackEndPlugin() {
        return new MockSearchBackEndPlugin(List.of(ParquetDataFormat.PARQUET_DATA_FORMAT_NAME));
    }

    @Override
    protected String dataFormatName() {
        return ParquetDataFormat.PARQUET_DATA_FORMAT_NAME;
    }

    @Override
    protected DocumentInput<?> createDocumentInput() {
        // Pre-populate with metadata fields that DataFormatAwareEngine.indexIntoEngine
        // will call updateField on. Without these, updateField throws "Field not found".
        ParquetDocumentInput input = new ParquetDocumentInput();
        input.addField(VERSION_FIELD, -1L);
        input.addField(SEQNO_FIELD, new SeqNoFieldMapper.SequenceIdentifiers(-2L, 0L));
        return input;
    }

    private Schema buildSchema() {
        List<Field> fields = new ArrayList<>();
        for (MappedFieldType ft : List.of(NAME_FIELD, AGE_FIELD)) {
            ParquetField pf = ArrowFieldRegistry.getParquetField(ft.typeName());
            fields.add(new Field(ft.name(), pf.getFieldType(), pf.children()));
        }
        // Version field (long)
        fields.add(new Field(VersionFieldMapper.NAME, FieldType.notNullable(new ArrowType.Int(64, true)), null));
        // SeqNo field (struct with seq_no + primary_term)
        SeqNoParquetField seqNoField = new SeqNoParquetField();
        fields.add(new Field(SeqNoFieldMapper.NAME, seqNoField.getFieldType(), seqNoField.children()));
        // Row ID field (long) — added by RowIdAwareWriter
        fields.add(new Field(DocumentInput.ROW_ID_FIELD, FieldType.notNullable(new ArrowType.Int(64, true)), null));
        return new Schema(fields);
    }
}

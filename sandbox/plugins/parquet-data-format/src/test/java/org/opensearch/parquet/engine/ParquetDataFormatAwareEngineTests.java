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
import org.apache.lucene.search.Query;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.AbstractDataFormatAwareEngineTestCase;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.stub.MockSearchBackEndPlugin;
import org.opensearch.index.mapper.BinaryFieldMapper.BinaryFieldType;
import org.opensearch.index.mapper.BooleanFieldMapper.BooleanFieldType;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper.DateFieldType;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.IpFieldMapper.IpFieldType;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MatchOnlyTextFieldMapper;
import org.opensearch.index.mapper.MatchOnlyTextFieldMapper.MatchOnlyTextFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper.TextFieldType;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.PrecomputedChecksumStrategy;
import org.opensearch.index.store.Store;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.fields.plugins.CoreDataFieldPlugin;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.parquet.engine.ParquetIndexingEngineTests.metadataFields;

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
 */
public class ParquetDataFormatAwareEngineTests extends AbstractDataFormatAwareEngineTestCase {

    private static final MappedFieldType NAME_FIELD = new KeywordFieldMapper.KeywordFieldType("name");
    private static final MappedFieldType AGE_FIELD = new NumberFieldMapper.NumberFieldType("age", NumberFieldMapper.NumberType.INTEGER);
    public static final MappedFieldType SEQ_NO_FIELD = new NumberFieldMapper.NumberFieldType(
        SeqNoFieldMapper.NAME,
        NumberFieldMapper.NumberType.LONG
    );
    public static final MappedFieldType VERSION_FIELD = new NumberFieldMapper.NumberFieldType(
        VersionFieldMapper.NAME,
        NumberFieldMapper.NumberType.LONG
    );
    public static final MappedFieldType ID_FIELD = new MappedFieldType(
        IdFieldMapper.CONTENT_TYPE,
        true,
        true,
        true,
        TextSearchInfo.SIMPLE_MATCH_ONLY,
        Map.of()
    ) {
        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            return null;
        }

        @Override
        public String typeName() {
            return IdFieldMapper.CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            return null;
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
                    () -> 1L,
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
        ParquetDocumentInput input = new ParquetDocumentInput();
        input.addField(ID_FIELD, "doc-id".getBytes(StandardCharsets.UTF_8));
        input.addField(NAME_FIELD, "name");
        input.addField(new NumberFieldMapper.NumberFieldType(BYTE_FIELD_NAME, NumberFieldMapper.NumberType.BYTE), Byte.MAX_VALUE);
        input.addField(new NumberFieldMapper.NumberFieldType(SHORT_FIELD_NAME, NumberFieldMapper.NumberType.SHORT), Short.MAX_VALUE);
        input.addField(new NumberFieldMapper.NumberFieldType(INT_FIELD_NAME, NumberFieldMapper.NumberType.INTEGER), Integer.MAX_VALUE);
        input.addField(new NumberFieldMapper.NumberFieldType(LONG_FIELD_NAME, NumberFieldMapper.NumberType.LONG), Long.MAX_VALUE);
        input.addField(new NumberFieldMapper.NumberFieldType(FLOAT_FIELD_NAME, NumberFieldMapper.NumberType.FLOAT), Float.MAX_VALUE);
        input.addField(new NumberFieldMapper.NumberFieldType(DOUBLE_FIELD_NAME, NumberFieldMapper.NumberType.DOUBLE), Double.MAX_VALUE);
        input.addField(
            new NumberFieldMapper.NumberFieldType(HALF_FLOAT_FIELD_NAME, NumberFieldMapper.NumberType.HALF_FLOAT),
            Short.MAX_VALUE
        );
        input.addField(
            new NumberFieldMapper.NumberFieldType(UNSIGNED_LONG_FIELD_NAME, NumberFieldMapper.NumberType.UNSIGNED_LONG),
            Long.MAX_VALUE
        );
        input.addField(new TextFieldType(TEXT_FIELD_NAME), randomAlphaOfLength(100));
        input.addField(new DateFieldType(DATE_FIELD_NAME), System.currentTimeMillis());
        input.addField(new DateFieldType(DATE_NANOS_FIELD_NAME, DateFieldMapper.Resolution.NANOSECONDS), System.nanoTime());
        input.addField(new IpFieldType(IP_FIELD_NAME), InetAddresses.forString("0.0.0.0"));
        input.addField(new BinaryFieldType(BINARY_FIELD_NAME), randomAlphaOfLength(100).getBytes(StandardCharsets.UTF_8));
        input.addField(new BooleanFieldType(BOOLEAN_FIELD_NAME), randomBoolean());
        input.addField(matchOnlyTextFieldType, randomAlphaOfLength(100));
        return input;
    }

    private static final String BYTE_FIELD_NAME = "byte_field";
    private static final String SHORT_FIELD_NAME = "short_field";
    private static final String INT_FIELD_NAME = "int_field";
    private static final String LONG_FIELD_NAME = "long_field";
    private static final String FLOAT_FIELD_NAME = "float_field";
    private static final String DOUBLE_FIELD_NAME = "double_field";
    private static final String HALF_FLOAT_FIELD_NAME = "half_float_field";
    private static final String UNSIGNED_LONG_FIELD_NAME = "unsigned_long_field";
    private static final String TEXT_FIELD_NAME = "text_field";
    private static final String DATE_FIELD_NAME = "date_field";
    private static final String DATE_NANOS_FIELD_NAME = "date_nanos_field";
    private static final String IP_FIELD_NAME = "ip_field";
    private static final String BINARY_FIELD_NAME = "binary_field";
    private static final String BOOLEAN_FIELD_NAME = "boolean_field";
    private static final String MATCH_ONLY_TEXT_FIELD_NAME = "match_only_text_field";

    MatchOnlyTextFieldType matchOnlyTextFieldType = new MatchOnlyTextFieldMapper.MatchOnlyTextFieldType(
        MATCH_ONLY_TEXT_FIELD_NAME,
        true,
        false,
        TextSearchInfo.SIMPLE_MATCH_ONLY,
        ParametrizedFieldMapper.Parameter.metaParam().get()
    );

    private Schema buildSchema() {
        List<Field> fields = new ArrayList<>();
        for (MappedFieldType ft : List.of(NAME_FIELD, AGE_FIELD)) {
            ParquetField pf = ArrowFieldRegistry.getParquetField(ft.typeName());
            fields.add(new Field(ft.name(), pf.getFieldType(), null));
        }
        for (Map.Entry<String, ParquetField> dataField : new CoreDataFieldPlugin().getParquetFields().entrySet()) {
            fields.add(new Field(dataField.getKey() + "_field", dataField.getValue().getFieldType(), null));
        }
        // Metadata fields (long, not nullable)
        fields.addAll(metadataFields());
        // Row ID field (long) — added by RowIdAwareWriter
        fields.add(new Field(DocumentInput.ROW_ID_FIELD, FieldType.notNullable(new ArrowType.Int(64, true)), null));
        return new Schema(fields);
    }
}

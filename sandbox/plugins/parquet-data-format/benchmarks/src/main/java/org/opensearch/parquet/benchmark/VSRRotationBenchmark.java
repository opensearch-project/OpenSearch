/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.benchmark;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.vsr.VSRManager;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark comparing synchronous vs asynchronous VSR rotation.
 *
 * <p>Measures end-to-end document ingestion throughput across multiple VSR rotations.
 * The {@code runAsync} parameter toggles between:
 * <ul>
 *   <li>{@code true} — frozen VSR writes happen on a background thread (post-optimization)</li>
 *   <li>{@code false} — frozen VSR writes block the calling thread (pre-optimization baseline)</li>
 * </ul>
 */
@Fork(1)
@Warmup(iterations = 2, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class VSRRotationBenchmark {

    @Param({ "true", "false" })
    private boolean runAsync;

    @Param({ "100000" })
    private int maxRowsPerVSR;

    @Param({ "10000000" })
    private int totalDocuments;

    @Param({ "20" })
    private int fieldCount;

    private ThreadPool threadPool;
    private ArrowBufferPool bufferPool;
    private Schema schema;
    private List<MappedFieldType> fieldTypes;
    private VSRManager vsrManager;
    private String filePath;
    private IndexSettings indexSettings;

    @Setup(Level.Trial)
    public void setupTrial() {
        RustBridge.initLogger();

        Settings settings = Settings.builder().put("node.name", "benchmark-node").build();
        threadPool = new ThreadPool(
            settings,
            new FixedExecutorBuilder(
                settings,
                ParquetDataFormatPlugin.PARQUET_THREAD_POOL_NAME,
                Runtime.getRuntime().availableProcessors(),
                -1,
                "thread_pool." + ParquetDataFormatPlugin.PARQUET_THREAD_POOL_NAME
            )
        );

        fieldTypes = new ArrayList<>();
        List<Field> arrowFields = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            MappedFieldType ft;
            switch (i % 3) {
                case 0:
                    ft = new NumberFieldMapper.NumberFieldType("int_field_" + i, NumberFieldMapper.NumberType.INTEGER);
                    break;
                case 1:
                    ft = new NumberFieldMapper.NumberFieldType("long_field_" + i, NumberFieldMapper.NumberType.LONG);
                    break;
                default:
                    ft = new KeywordFieldMapper.KeywordFieldType("keyword_field_" + i);
                    break;
            }
            fieldTypes.add(ft);
            ParquetField pf = ArrowFieldRegistry.getParquetField(ft.typeName());
            arrowFields.add(new Field(ft.name(), pf.getFieldType(), null));
        }
        schema = new Schema(arrowFields);
    }

    @Setup(Level.Invocation)
    public void setup() throws IOException {
        bufferPool = new ArrowBufferPool(Settings.EMPTY);
        filePath = Path.of(System.getProperty("java.io.tmpdir"), "benchmark_vsr_" + System.nanoTime() + ".parquet").toString();
        Settings idxSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexMetadata indexMetadata = IndexMetadata.builder("benchmark-index").settings(idxSettings).build();
        indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        vsrManager = new VSRManager(filePath, indexSettings, schema, bufferPool, maxRowsPerVSR, threadPool, runAsync);
    }

    @Benchmark
    public void benchmarkDocumentIngestion() throws IOException {
        for (int i = 0; i < totalDocuments; i++) {
            ParquetDocumentInput doc = new ParquetDocumentInput();
            for (int f = 0; f < fieldTypes.size(); f++) {
                MappedFieldType ft = fieldTypes.get(f);
                switch (f % 3) {
                    case 0:
                        doc.addField(ft, i);
                        break;
                    case 1:
                        doc.addField(ft, (long) i * 100);
                        break;
                    default:
                        doc.addField(ft, "value_" + i);
                        break;
                }
            }
            vsrManager.addDocument(doc);
        }
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
        try {
            vsrManager.flush();
        } catch (Exception ignored) {}
        try {
            vsrManager.close();
        } catch (Exception ignored) {}
        try {
            Files.deleteIfExists(Path.of(filePath));
        } catch (Exception ignored) {}
        bufferPool.close();
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        threadPool.shutdownNow();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.benchmark;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetColumnReader;
import org.opensearch.parquet.bridge.ParquetSortConfig;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.codec.ParquetPhysicalType;
import org.opensearch.parquet.codec.cache.BufferPool;
import org.opensearch.parquet.codec.iter.ParquetNumericDocValues;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark for the {@link ParquetNumericDocValues#advanceExact} hot path over a
 * single-valued INT64 Parquet column ("price").
 *
 * <p>Each invocation is one full pass over the column:
 * <ul>
 *   <li>{@code sequentialScan} — calls {@code advanceExact(d)} for every doc
 *       {@code 0..maxDoc-1}. Almost every call is a resident {@code PageCache} hit; a page
 *       miss (FFM decode crossing) happens only at page boundaries, so this measures the
 *       Layer 1/2 bit-test + array-lookup fast path.</li>
 *   <li>{@code stridedScan} — calls {@code advanceExact(d)} for every {@code stride}-th doc.
 *       Larger strides skip more rows per resident page (and eventually a whole page per
 *       call), shifting the hit/miss mix toward the page-decode cold path.</li>
 * </ul>
 *
 * <p>The reader is opened once per trial ({@link Setup} at {@link Level#Trial}), so
 * measurements reflect warm scans over an already-open column reader; the file open and
 * page-index load are excluded from the measured region.
 *
 * <p>Run with:
 * <pre>
 * ./gradlew -Dsandbox.enabled=true :sandbox:plugins:parquet-data-format:benchmarks:run \
 *   --args 'AdvanceExactBenchmark'
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class AdvanceExactBenchmark {

    /** Rows per Arrow batch handed to the native writer during setup. */
    private static final int BATCH_ROWS = 100_000;

    /**
     * Shared per-trial state: the Parquet file, the open column reader, and the doc-values
     * iterator. Parameterized on {@code numDocs} only, so {@code sequentialScan} does not
     * re-run per stride value.
     */
    @State(Scope.Benchmark)
    public static class ScanState {

        @Param({ "100000", "1000000" })
        int numDocs;

        BufferAllocator allocator;
        Path dir;
        Path file;
        BufferPool bufferPool;
        ParquetColumnReader reader;
        ParquetNumericDocValues docValues;

        @Setup(Level.Trial)
        public void setupTrial() throws Exception {
            RustBridge.initLogger();
            allocator = new RootAllocator();
            dir = Files.createTempDirectory("advance-exact-bench");
            file = dir.resolve("bench.parquet");
            writeFile();

            bufferPool = new BufferPool();
            reader = ParquetColumnReader.open(file, "price", ParquetPhysicalType.INT64, false, bufferPool);
            docValues = new ParquetNumericDocValues(reader, numDocs);
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() throws Exception {
            if (reader != null) {
                reader.close();
            }
            if (bufferPool != null) {
                bufferPool.close();
            }
            if (allocator != null) {
                allocator.close();
            }
            if (file != null) {
                Files.deleteIfExists(file);
            }
            if (dir != null) {
                Files.deleteIfExists(dir);
            }
        }

        // ── data generation ──

        private void writeFile() throws Exception {
            Schema schema = new Schema(List.of(new Field("price", FieldType.nullable(new ArrowType.Int(64, true)), null)));

            NativeParquetWriter writer = new NativeParquetWriter(file.toString());
            try (ArrowExport schemaExport = exportSchema(schema)) {
                writer.initialize("advance-exact-bench-index", schemaExport.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
            }

            // Deterministic values so every fork/param combination measures identical data.
            Random random = new Random(42);
            for (int start = 0; start < numDocs; start += BATCH_ROWS) {
                int batch = Math.min(BATCH_ROWS, numDocs - start);
                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                    BigIntVector priceVec = (BigIntVector) root.getVector("price");
                    for (int i = 0; i < batch; i++) {
                        priceVec.setSafe(i, random.nextLong());
                    }
                    root.setRowCount(batch);

                    ArrowArray array = ArrowArray.allocateNew(allocator);
                    ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
                    Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
                    try (ArrowExport export = new ArrowExport(array, arrowSchema)) {
                        writer.write(export.getArrayAddress(), export.getSchemaAddress());
                    }
                }
            }
            writer.flush();
        }

        private ArrowExport exportSchema(Schema schema) {
            ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
            Data.exportSchema(allocator, schema, null, arrowSchema);
            return new ArrowExport(null, arrowSchema);
        }
    }

    /** Stride between consecutive advanceExact targets; only applies to {@link #stridedScan}. */
    @State(Scope.Benchmark)
    public static class StrideParam {

        @Param({ "1", "100", "10000" })
        int stride;
    }

    /**
     * Full ascending scan: exercises the page-hit fast path heavily — the only page misses
     * are at page boundaries (~rows/page_row_limit FFM decodes per scan).
     */
    @Benchmark
    public void sequentialScan(ScanState state, Blackhole bh) throws IOException {
        ParquetNumericDocValues dv = state.docValues;
        for (int d = 0; d < state.numDocs; d++) {
            if (dv.advanceExact(d)) {
                bh.consume(dv.longValue());
            }
        }
    }

    /**
     * Ascending scan visiting every {@code stride}-th doc: fewer hits are amortized over each
     * page decode, raising the page-miss share of the per-call cost.
     */
    @Benchmark
    public void stridedScan(ScanState state, StrideParam strideParam, Blackhole bh) throws IOException {
        ParquetNumericDocValues dv = state.docValues;
        for (int d = 0; d < state.numDocs; d += strideParam.stride) {
            if (dv.advanceExact(d)) {
                bh.consume(dv.longValue());
            }
        }
    }
}

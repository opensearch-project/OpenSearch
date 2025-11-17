/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.benchmark;

import com.parquet.parquetdataformat.bridge.RustBridge;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class ParquetWriterWriteBenchmark {

    private BenchmarkData writerCreationBenchmarkData;
    private BenchmarkData writerWriteBenchmarkData;
    private String filePath;

    @Param({"10"})
    private int fieldCount;

    @Param({"50000"})
    private int recordCount;

    @Setup(Level.Invocation)
    public void setup() throws IOException {
        BenchmarkDataGenerator generator = new BenchmarkDataGenerator();
        writerCreationBenchmarkData = generator.generate("simple", fieldCount, 0);
        writerWriteBenchmarkData = generator.generate("simple", fieldCount, recordCount);
        filePath = generateTempFilePath();
        RustBridge.createWriter(filePath, writerCreationBenchmarkData.getArrowSchema().memoryAddress());
    }

    @Benchmark
    public void benchmarkWrite() throws IOException {
        RustBridge.write(filePath, writerWriteBenchmarkData.getArrowArray().memoryAddress(), writerWriteBenchmarkData.getArrowSchema().memoryAddress());
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws IOException {
        RustBridge.closeWriter(filePath);
        writerCreationBenchmarkData.close();
        writerWriteBenchmarkData.close();
    }

    private String generateTempFilePath() {
        return Path.of(System.getProperty("java.io.tmpdir"), "benchmark_writer_" + System.nanoTime() + ".parquet").toString();
    }
}

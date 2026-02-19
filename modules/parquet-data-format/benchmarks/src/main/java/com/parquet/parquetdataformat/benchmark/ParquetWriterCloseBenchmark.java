/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.benchmark;

import com.parquet.parquetdataformat.bridge.RustBridge;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
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
import java.util.concurrent.TimeUnit;

/**
 * Simple JMH benchmark for testing Parquet writer creation performance.
 * This benchmark focuses specifically on measuring the overhead of creating writers.
 */
@Fork(1)
@Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class ParquetWriterCloseBenchmark {

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
        RustBridge.write(filePath, writerWriteBenchmarkData.getArrowArray().memoryAddress(), writerWriteBenchmarkData.getArrowSchema().memoryAddress());
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws IOException {
        try {
            Files.deleteIfExists(Path.of(filePath));
        } catch (Exception ignored) {
            // Best effort cleanup
        }

        writerCreationBenchmarkData.close();
        writerWriteBenchmarkData.close();
    }


    @Benchmark
    public void benchmarkClose() throws IOException {
        RustBridge.closeWriter(filePath);
    }

    private String generateTempFilePath() {
        return System.getProperty("java.io.tmpdir") + "/benchmark_writer_" +
            System.nanoTime() + ".parquet";
    }
}

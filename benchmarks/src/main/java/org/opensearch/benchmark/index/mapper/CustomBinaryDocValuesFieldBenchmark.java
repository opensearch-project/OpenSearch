/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.BinaryFieldMapper;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 1)
@Measurement(iterations = 1)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class CustomBinaryDocValuesFieldBenchmark {

    static final String FIELD_NAME = "dummy";
    static final String SEED_VALUE = "seed";

    @Benchmark
    public void add(CustomBinaryDocValuesFieldBenchmark.BenchmarkParameters parameters, Blackhole blackhole) {
        // Don't use the parameter binary doc values object.
        // Start with a fresh object every call and add maximum number of entries
        BinaryFieldMapper.CustomBinaryDocValuesField customBinaryDocValuesField = new BinaryFieldMapper.CustomBinaryDocValuesField(
            FIELD_NAME,
            new BytesRef(SEED_VALUE).bytes
        );
        for (int i = 0; i < parameters.maximumNumberOfEntries; ++i) {
            ThreadLocalRandom.current().nextBytes(parameters.bytes);
            customBinaryDocValuesField.add(parameters.bytes);
        }
    }

    @Benchmark
    public void binaryValue(CustomBinaryDocValuesFieldBenchmark.BenchmarkParameters parameters, Blackhole blackhole) {
        blackhole.consume(parameters.customBinaryDocValuesField.binaryValue());
    }

    @State(Scope.Benchmark)
    public static class BenchmarkParameters {
        @Param({ "8", "32", "128", "512" })
        int maximumNumberOfEntries;

        @Param({ "8", "32", "128", "512" })
        int entrySize;

        BinaryFieldMapper.CustomBinaryDocValuesField customBinaryDocValuesField;
        byte[] bytes;

        @Setup
        public void setup() {
            customBinaryDocValuesField = new BinaryFieldMapper.CustomBinaryDocValuesField(FIELD_NAME, new BytesRef(SEED_VALUE).bytes);
            bytes = new byte[entrySize];
            for (int i = 0; i < maximumNumberOfEntries; ++i) {
                ThreadLocalRandom.current().nextBytes(bytes);
                customBinaryDocValuesField.add(bytes);
            }
        }
    }
}

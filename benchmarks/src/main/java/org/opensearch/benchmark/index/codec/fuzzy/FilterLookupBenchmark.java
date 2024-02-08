/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.index.codec.fuzzy;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.UUIDs;
import org.opensearch.index.codec.fuzzy.FuzzySet;
import org.opensearch.index.codec.fuzzy.FuzzySetFactory;
import org.opensearch.index.codec.fuzzy.FuzzySetParameters;
import org.opensearch.index.mapper.IdFieldMapper;
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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Fork(3)
@Warmup(iterations = 2)
@Measurement(iterations = 5, time = 60, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class FilterLookupBenchmark {

    @Param({ "50000000", "1000000" })
    private int numItems;

    @Param({ "1000000" })
    private int searchKeyCount;

    @Param({ "0.0511", "0.1023", "0.2047" })
    private double fpp;

    private FuzzySet fuzzySet;
    private List<BytesRef> items;
    private Random random = new Random();

    @Setup
    public void setupFilter() throws IOException {
        String fieldName = IdFieldMapper.NAME;
        items = IntStream.range(0, numItems).mapToObj(i -> new BytesRef(UUIDs.base64UUID())).collect(Collectors.toList());
        FuzzySetParameters parameters = new FuzzySetParameters(() -> fpp);
        fuzzySet = new FuzzySetFactory(Map.of(fieldName, parameters)).createFuzzySet(numItems, fieldName, () -> items.iterator());
    }

    @Benchmark
    public void contains_withExistingKeys(Blackhole blackhole) throws IOException {
        for (int i = 0; i < searchKeyCount; i++) {
            blackhole.consume(fuzzySet.contains(items.get(random.nextInt(items.size()))) == FuzzySet.Result.MAYBE);
        }
    }

    @Benchmark
    public void contains_withRandomKeys(Blackhole blackhole) throws IOException {
        for (int i = 0; i < searchKeyCount; i++) {
            blackhole.consume(fuzzySet.contains(new BytesRef(UUIDs.base64UUID())));
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.core.common.collect;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
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
import org.opensearch.core.common.collect.ImmutableOpenMap;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Fork(2)
@Warmup(iterations = 1)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class ImmutableOpenMapBenchmark {
    ImmutableOpenMap<Integer, Double> map;

    @Param({ "10000" })
    public int count;

    @Setup
    public void buildMaps() {
        ImmutableOpenMap.Builder<Integer, Double> mb = new ImmutableOpenMap.Builder<Integer, Double>(count);
        for (int i = 0; i < count; ++i) {
            mb.put(i, ThreadLocalRandom.current().nextDouble());
        }
        map = mb.build();
    }

    @Benchmark
    public void put(Blackhole blackhole) {
        ImmutableOpenMap.Builder<Integer, Double> mb = new ImmutableOpenMap.Builder<Integer, Double>(count);
        for (int i = 0; i < count; ++i) {
            blackhole.consume(mb.put(i, ThreadLocalRandom.current().nextDouble()));
        }
    }

    @Benchmark
    public void getHPPC(Blackhole blackhole) {
        for (ObjectObjectCursor<Integer, Double> cursor : map) {
            blackhole.consume(cursor.value);
        }
    }

    @Benchmark
    public void getJavaMap(Blackhole blackhole) {
        for (Map.Entry<Integer, Double> entry : map.entrySet()) {
            blackhole.consume(entry.getValue());
        }
    }
}

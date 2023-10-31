/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.round;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.function.Supplier;

@Fork(value = 3)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 1, time = 1)
@BenchmarkMode(Mode.Throughput)
public class RoundableBenchmark {

    @Benchmark
    public void floor(Blackhole bh, Options opts) {
        Roundable roundable = opts.supplier.get();
        for (long key : opts.queries) {
            bh.consume(roundable.floor(key));
        }
    }

    @State(Scope.Benchmark)
    public static class Options {
        @Param({
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "10",
            "12",
            "14",
            "16",
            "18",
            "20",
            "22",
            "24",
            "26",
            "29",
            "32",
            "37",
            "41",
            "45",
            "49",
            "54",
            "60",
            "64",
            "74",
            "83",
            "90",
            "98",
            "108",
            "118",
            "128",
            "144",
            "159",
            "171",
            "187",
            "204",
            "229",
            "256" })
        public Integer size;

        @Param({ "binary", "linear" })
        public String type;

        @Param({ "uniform", "skewed_edge", "skewed_center" })
        public String distribution;

        public long[] queries;
        public Supplier<Roundable> supplier;

        @Setup
        public void setup() {
            Random random = new Random(size);
            long[] values = new long[size];
            for (int i = 1; i < values.length; i++) {
                values[i] = values[i - 1] + 100;
            }

            long range = values[values.length - 1] - values[0] + 100;
            long mean, stddev;
            queries = new long[1000000];

            switch (distribution) {
                case "uniform": // all values equally likely.
                    for (int i = 0; i < queries.length; i++) {
                        queries[i] = values[0] + (nextPositiveLong(random) % range);
                    }
                    break;
                case "skewed_edge": // distribution centered at p90 with ± 5% stddev.
                    mean = values[0] + (long) (range * 0.9);
                    stddev = (long) (range * 0.05);
                    for (int i = 0; i < queries.length; i++) {
                        queries[i] = Math.max(values[0], mean + (long) (random.nextGaussian() * stddev));
                    }
                    break;
                case "skewed_center": // distribution centered at p50 with ± 5% stddev.
                    mean = values[0] + (long) (range * 0.5);
                    stddev = (long) (range * 0.05);
                    for (int i = 0; i < queries.length; i++) {
                        queries[i] = Math.max(values[0], mean + (long) (random.nextGaussian() * stddev));
                    }
                    break;
                default:
                    throw new IllegalArgumentException("invalid distribution: " + distribution);
            }

            switch (type) {
                case "binary":
                    supplier = () -> new BinarySearcher(values, size);
                    break;
                case "linear":
                    supplier = () -> new BidirectionalLinearSearcher(values, size);
                    break;
                default:
                    throw new IllegalArgumentException("invalid type: " + type);
            }
        }

        private static long nextPositiveLong(Random random) {
            return random.nextLong() & Long.MAX_VALUE;
        }
    }
}

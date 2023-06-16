/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

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
import org.opensearch.common.lease.Releasable;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Fork(value = 3)
@Warmup(iterations = 1, time = 4)
@Measurement(iterations = 3, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class LongHashBenchmark {

    @Benchmark
    public void add(Blackhole bh, HashTableOptions tableOpts, WorkloadOptions workloadOpts) {
        try (HashTable table = tableOpts.get(); WorkloadIterator iter = workloadOpts.iter()) {
            while (iter.hasNext()) {
                bh.consume(table.add(iter.next()));
            }
        }
    }

    /**
     * Creates a hash table with varying parameters.
     */
    @State(Scope.Benchmark)
    public static class HashTableOptions {

        @Param({ "LongHash", "ReorganizingLongHash" })
        public String type;

        @Param({ "1" })
        public long initialCapacity;

        @Param({ "0.6" })
        public float loadFactor;

        private Supplier<HashTable> supplier;

        @Setup
        public void setup() {
            switch (type) {
                case "LongHash":
                    supplier = this::newLongHash;
                    break;
                case "ReorganizingLongHash":
                    supplier = this::newReorganizingLongHash;
                    break;
                default:
                    throw new IllegalArgumentException("invalid hash table type: " + type);
            }
        }

        public HashTable get() {
            return supplier.get();
        }

        private HashTable newLongHash() {
            return new HashTable() {
                private final LongHash table = new LongHash(initialCapacity, loadFactor, BigArrays.NON_RECYCLING_INSTANCE);

                @Override
                public long add(long key) {
                    return table.add(key);
                }

                @Override
                public void close() {
                    table.close();
                }
            };
        }

        private HashTable newReorganizingLongHash() {
            return new HashTable() {
                private final ReorganizingLongHash table = new ReorganizingLongHash(
                    initialCapacity,
                    loadFactor,
                    BigArrays.NON_RECYCLING_INSTANCE
                );

                @Override
                public long add(long key) {
                    return table.add(key);
                }

                @Override
                public void close() {
                    table.close();
                }
            };
        }
    }

    /**
     * Creates a workload with varying parameters.
     */
    @State(Scope.Benchmark)
    public static class WorkloadOptions {
        public static final int NUM_HITS = 20_000_000;

        /**
         * Repeat the experiment with growing number of keys.
         * These values are generated with an exponential growth pattern such that:
         * value = ceil(previous_value * random_float_between(1.0, 1.14))
         */
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
            "11",
            "13",
            "15",
            "17",
            "18",
            "19",
            "20",
            "21",
            "23",
            "26",
            "27",
            "30",
            "32",
            "35",
            "41",
            "45",
            "50",
            "53",
            "54",
            "55",
            "57",
            "63",
            "64",
            "69",
            "74",
            "80",
            "84",
            "91",
            "98",
            "101",
            "111",
            "114",
            "124",
            "128",
            "139",
            "148",
            "161",
            "162",
            "176",
            "190",
            "204",
            "216",
            "240",
            "257",
            "269",
            "291",
            "302",
            "308",
            "327",
            "341",
            "374",
            "402",
            "412",
            "438",
            "443",
            "488",
            "505",
            "558",
            "612",
            "621",
            "623",
            "627",
            "642",
            "717",
            "765",
            "787",
            "817",
            "915",
            "962",
            "1011",
            "1083",
            "1163",
            "1237",
            "1301",
            "1424",
            "1541",
            "1716",
            "1805",
            "1817",
            "1934",
            "2024",
            "2238",
            "2281",
            "2319",
            "2527",
            "2583",
            "2639",
            "2662",
            "2692",
            "2991",
            "3201",
            "3215",
            "3517",
            "3681",
            "3710",
            "4038",
            "4060",
            "4199",
            "4509",
            "4855",
            "5204",
            "5624",
            "6217",
            "6891",
            "7569",
            "8169",
            "8929",
            "9153",
            "10005",
            "10624",
            "10931",
            "12070",
            "12370",
            "13694",
            "14227",
            "15925",
            "17295",
            "17376",
            "18522",
            "19200",
            "20108",
            "21496",
            "23427",
            "24224",
            "26759",
            "29199",
            "29897",
            "32353",
            "33104",
            "36523",
            "38480",
            "38958",
            "40020",
            "44745",
            "45396",
            "47916",
            "49745",
            "49968",
            "52231",
            "53606" })
        public int size;

        @Param({ "correlated", "uncorrelated", "distinct" })
        public String dataset;

        private WorkloadIterator iterator;

        @Setup
        public void setup() {
            switch (dataset) {
                case "correlated":
                    iterator = newCorrelatedWorkload();
                    break;
                case "uncorrelated":
                    iterator = newUncorrelatedWorkload();
                    break;
                case "distinct":
                    iterator = newDistinctWorkload();
                    break;
                default:
                    throw new IllegalArgumentException("invalid dataset: " + dataset);
            }
        }

        public WorkloadIterator iter() {
            return iterator;
        }

        /**
         * Simulates monotonically increasing timestamp data with multiple hits mapping to the same key.
         */
        private WorkloadIterator newCorrelatedWorkload() {
            assert NUM_HITS >= size : "ensure hits >= size so that each key is used at least once";

            final long[] data = new long[size];
            for (int i = 0; i < data.length; i++) {
                data[i] = 1420070400000L + 3600000L * i;
            }

            return new WorkloadIterator() {
                private int count = 0;
                private int index = 0;
                private int remaining = NUM_HITS / data.length;

                @Override
                public boolean hasNext() {
                    return count < NUM_HITS;
                }

                @Override
                public long next() {
                    if (--remaining <= 0) {
                        index = (index + 1) % data.length;
                        remaining = NUM_HITS / data.length;
                    }
                    count++;
                    return data[index];
                }

                @Override
                public void reset() {
                    count = 0;
                    index = 0;
                    remaining = NUM_HITS / data.length;
                }
            };
        }

        /**
         * Simulates uncorrelated data (such as travel distance / fare amount).
         */
        private WorkloadIterator newUncorrelatedWorkload() {
            assert NUM_HITS >= size : "ensure hits >= size so that each key is used at least once";

            final Random random = new Random(0); // fixed seed for reproducible results
            final long[] data = new long[size];
            for (int i = 0; i < data.length; i++) {
                data[i] = Double.doubleToLongBits(20.0 + 80 * random.nextDouble());
            }

            return new WorkloadIterator() {
                private int count = 0;
                private int index = 0;

                @Override
                public boolean hasNext() {
                    return count < NUM_HITS;
                }

                @Override
                public long next() {
                    count++;
                    index = (index + 1) % data.length;
                    return data[index];
                }

                @Override
                public void reset() {
                    count = 0;
                    index = 0;
                }
            };
        }

        /**
         * Simulates workload with high cardinality, i.e., each hit mapping to a different key.
         */
        private WorkloadIterator newDistinctWorkload() {
            return new WorkloadIterator() {
                private int count = 0;

                @Override
                public boolean hasNext() {
                    return count < size;
                }

                @Override
                public long next() {
                    return count++;
                }

                @Override
                public void reset() {
                    count = 0;
                }
            };
        }
    }

    private interface HashTable extends Releasable {
        long add(long key);
    }

    private interface WorkloadIterator extends Releasable {
        boolean hasNext();

        long next();

        void reset();

        @Override
        default void close() {
            reset();
        }
    }
}

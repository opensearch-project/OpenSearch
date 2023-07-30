/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.apache.lucene.util.BytesRef;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.common.lease.Releasables;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Fork(value = 3)
@Warmup(iterations = 1, time = 2)
@Measurement(iterations = 3, time = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BytesRefHashBenchmark {
    private static final int NUM_TABLES = 20;  // run across many tables so that caches aren't effective
    private static final int NUM_HITS = 1_000_000;  // num hits per table

    @Benchmark
    public void add(Blackhole bh, Options opts) {
        for (int hit = 0; hit < NUM_HITS; hit++) {
            BytesRef key = opts.keys[hit % opts.keys.length];
            for (BytesRefHash table : opts.tables) {
                bh.consume(table.add(key));
            }
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
            "10",
            "12",
            "14",
            "16",
            "19",
            "22",
            "25",
            "29",
            "33",
            "38",
            "43",
            "50",
            "57",
            "65",
            "75",
            "86",
            "97",
            "109",
            "124",
            "141",
            "161",
            "182",
            "204",
            "229",
            "262",
            "297",
            "336",
            "380",
            "430",
            "482",
            "550",
            "610",
            "704",
            "801",
            "914",
            "1042",
            "1178",
            "1343",
            "1532",
            "1716",
            "1940",
            "2173",
            "2456",
            "2751",
            "3082",
            "3514",
            "4006",
            "4487",
            "5026",
            "5730",
            "6418",
            "7317",
            "8196",
            "9180",
            "10374",
            "11723",
            "13247",
            "14837",
            "16915",
            "19114",
            "21599",
            "24623",
            "28071",
            "32001",
            "36482",
            "41590",
            "46581",
            "52637",
            "58954",
            "67208",
            "76618",
            "86579",
            "97835",
            "109576",
            "122726",
            "138681",
            "156710",
            "175516",
            "198334",
            "222135",
            "248792",
            "281135",
            "320494",
            "365364",
            "409208",
            "466498",
            "527143",
            "595672",
            "667153",
            "753883",
            "851888",
            "971153" })

        public Integer size;

        @Param({ "8", "32", "128" })
        public Integer length;

        private BytesRefHash[] tables;

        private BytesRef[] keys;

        @Setup
        public void setup() {
            assert size <= Math.pow(26, length) : "key length too small to generate the required number of keys";
            tables = Stream.generate(() -> new BytesRefHash(BigArrays.NON_RECYCLING_INSTANCE))
                .limit(NUM_TABLES)
                .toArray(BytesRefHash[]::new);
            Random random = new Random(0);
            Set<BytesRef> seen = new HashSet<>();
            keys = new BytesRef[size];
            for (int i = 0; i < size; i++) {
                BytesRef key;
                do {
                    key = new BytesRef(
                        random.ints(97, 123)
                            .limit(length)
                            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                            .toString()
                    );
                } while (seen.contains(key));
                keys[i] = key;
                seen.add(key);
            }
        }

        @TearDown
        public void tearDown() {
            Releasables.close(tables);
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.hash;

import org.apache.lucene.util.StringHelper;
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

@Fork(value = 3)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 3, time = 3)
@BenchmarkMode(Mode.Throughput)
public class HashFunctionBenchmark {

    @Benchmark
    public void hash(Blackhole bh, Options opts) {
        bh.consume(opts.type.hash(opts.data));
    }

    @State(Scope.Benchmark)
    public static class Options {
        @Param({ "MURMUR3", "T1HA1" })
        public Type type;

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
            "21",
            "24",
            "28",
            "32",
            "36",
            "41",
            "47",
            "54",
            "62",
            "71",
            "81",
            "90",
            "100",
            "112",
            "125",
            "139",
            "156",
            "174",
            "194",
            "220",
            "245",
            "272",
            "302",
            "339",
            "384",
            "431",
            "488",
            "547",
            "608",
            "675",
            "763",
            "863",
            "967",
            "1084",
            "1225",
            "1372",
            "1537",
            "1737",
            "1929",
            "2142",
            "2378",
            "2664",
            "3011",
            "3343",
            "3778",
            "4232",
            "4783",
            "5310",
            "5895",
            "6662",
            "7529",
            "8508",
            "9444",
            "10483",
            "11741",
            "13150",
            "14597",
            "16495",
            "18475",
            "20877",
            "23383",
            "25956",
            "29071",
            "32560",
            "36142",
            "40841",
            "46151",
            "52151",
            "57888",
            "65414",
            "72610",
            "82050",
            "91076",
            "102006",
            "114247",
            "127957",
            "143312",
            "159077",
            "176576",
            "199531",
            "223475",
            "250292",
            "277825",
            "313943",
            "351617",
            "393812" })
        public Integer length;
        public byte[] data;

        @Setup
        public void setup() {
            data = new byte[length];
            new Random(0).nextBytes(data);
        }
    }

    public enum Type {
        MURMUR3((data, offset, length) -> StringHelper.murmurhash3_x86_32(data, offset, length, 0)),
        T1HA1((data, offset, length) -> T1ha1.hash(data, offset, length, 0));

        private final Hasher hasher;

        Type(Hasher hasher) {
            this.hasher = hasher;
        }

        public long hash(byte[] data) {
            return hasher.hash(data, 0, data.length);
        }
    }

    @FunctionalInterface
    interface Hasher {
        long hash(byte[] data, int offset, int length);
    }
}

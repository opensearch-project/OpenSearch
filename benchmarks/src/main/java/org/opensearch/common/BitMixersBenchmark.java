/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.common.hash.BitMixers;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Fork(3)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class BitMixersBenchmark {
    private final AtomicLong val = new AtomicLong();

    @Benchmark
    public void identity(Blackhole bh) {
        bh.consume(BitMixers.IDENTITY.mix(val.incrementAndGet()));
    }

    @Benchmark
    public void phi(Blackhole bh) {
        bh.consume(BitMixers.PHI.mix(val.incrementAndGet()));
    }

    @Benchmark
    public void fasthash(Blackhole bh) {
        bh.consume(BitMixers.FASTHASH.mix(val.incrementAndGet()));
    }

    @Benchmark
    public void xxhash(Blackhole bh) {
        bh.consume(BitMixers.XXHASH.mix(val.incrementAndGet()));
    }

    @Benchmark
    public void mxm(Blackhole bh) {
        bh.consume(BitMixers.MXM.mix(val.incrementAndGet()));
    }

    @Benchmark
    public void murmur3(Blackhole bh) {
        bh.consume(BitMixers.MURMUR3.mix(val.incrementAndGet()));
    }
}

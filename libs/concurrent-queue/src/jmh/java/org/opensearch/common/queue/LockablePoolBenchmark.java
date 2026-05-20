/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.queue;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * JMH benchmark for {@link LockablePool} measuring:
 * <ul>
 *   <li>Isolated checkout/return throughput at varying thread counts</li>
 *   <li>Mixed workload: concurrent writers + periodic checkoutAll (refresh)</li>
 *   <li>Aggressive refresh: frequent checkoutAll to stress Phase 3 bulk cleanup</li>
 *   <li>Writer latency during refresh contention (sample mode)</li>
 * </ul>
 */
@Fork(2)
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 3, time = 5)
@State(Scope.Benchmark)
@SuppressWarnings("unused")
public class LockablePoolBenchmark {

    @Param({ "4", "8" })
    int concurrency;

    private LockablePool<PoolEntry> pool;

    @Setup(Level.Iteration)
    public void setup() {
        AtomicInteger counter = new AtomicInteger(0);
        pool = new LockablePool<>(() -> new PoolEntry(counter.getAndIncrement()), LinkedList::new, concurrency);
        // Pre-warm the pool with more entries to populate multiple stripes
        for (int i = 0; i < concurrency * 4; i++) {
            PoolEntry e = pool.getAndLock();
            pool.releaseAndUnlock(e);
        }
    }

    // ── Mixed workload: writers + periodic refresh (1s interval) ──

    @Benchmark
    @Group("mixed_7w_1r")
    @GroupThreads(7)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void writers_7w1r(Blackhole bh) {
        PoolEntry e = pool.getAndLock();
        bh.consume(simulateWork(e));
        pool.releaseAndUnlock(e);
    }

    @Benchmark
    @Group("mixed_7w_1r")
    @GroupThreads(1)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public List<PoolEntry> refresh_7w1r() throws InterruptedException {
        Thread.sleep(1000);
        return pool.checkoutAll();
    }

    @Benchmark
    @Group("mixed_3w_1r")
    @GroupThreads(3)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void writers_3w1r(Blackhole bh) {
        PoolEntry e = pool.getAndLock();
        bh.consume(simulateWork(e));
        pool.releaseAndUnlock(e);
    }

    @Benchmark
    @Group("mixed_3w_1r")
    @GroupThreads(1)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public List<PoolEntry> refresh_3w1r() throws InterruptedException {
        Thread.sleep(1000);
        return pool.checkoutAll();
    }

    // ── Aggressive refresh: 10ms interval to stress checkoutAll Phase 3 ──
    // This makes checkoutAll fire ~100x/sec instead of 1x/sec, amplifying
    // the difference between per-item remove (old) and bulk removeIf (new).

    @Benchmark
    @Group("aggressive_7w_1r")
    @GroupThreads(7)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void writers_aggressive_7w1r(Blackhole bh) {
        PoolEntry e = pool.getAndLock();
        bh.consume(simulateWork(e));
        pool.releaseAndUnlock(e);
    }

    @Benchmark
    @Group("aggressive_7w_1r")
    @GroupThreads(1)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public List<PoolEntry> refresh_aggressive_7w1r() {
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        return pool.checkoutAll();
    }

    @Benchmark
    @Group("aggressive_3w_1r")
    @GroupThreads(3)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void writers_aggressive_3w1r(Blackhole bh) {
        PoolEntry e = pool.getAndLock();
        bh.consume(simulateWork(e));
        pool.releaseAndUnlock(e);
    }

    @Benchmark
    @Group("aggressive_3w_1r")
    @GroupThreads(1)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public List<PoolEntry> refresh_aggressive_3w1r() {
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        return pool.checkoutAll();
    }

    // ── Writer latency during refresh contention (sample mode) ──
    // Measures per-operation latency distribution to capture tail latency
    // spikes caused by checkoutAll holding the pool lock.

    @Benchmark
    @Group("latency_7w_1r")
    @GroupThreads(7)
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void writers_latency_7w1r(Blackhole bh) {
        PoolEntry e = pool.getAndLock();
        bh.consume(simulateWork(e));
        pool.releaseAndUnlock(e);
    }

    @Benchmark
    @Group("latency_7w_1r")
    @GroupThreads(1)
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public List<PoolEntry> refresh_latency_7w1r() {
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        return pool.checkoutAll();
    }

    // ── Isolated: pure writer throughput (no refresh contention) ──

    @Benchmark
    @Group("writers_only_4t")
    @GroupThreads(4)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void writersOnly_4t(Blackhole bh) {
        PoolEntry e = pool.getAndLock();
        bh.consume(simulateWork(e));
        pool.releaseAndUnlock(e);
    }

    @Benchmark
    @Group("writers_only_8t")
    @GroupThreads(8)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void writersOnly_8t(Blackhole bh) {
        PoolEntry e = pool.getAndLock();
        bh.consume(simulateWork(e));
        pool.releaseAndUnlock(e);
    }

    // ── Isolated checkoutAll throughput (no writers) ──
    // Directly measures checkoutAll cost with a pre-populated pool.

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public List<PoolEntry> checkoutAll_isolated() {
        // Re-populate pool before each checkout
        for (int i = 0; i < concurrency; i++) {
            PoolEntry e = pool.getAndLock();
            pool.releaseAndUnlock(e);
        }
        return pool.checkoutAll();
    }

    private static long simulateWork(PoolEntry entry) {
        long result = entry.hashCode();
        for (int i = 0; i < 20; i++) {
            result ^= (result << 13);
            result ^= (result >> 7);
            result ^= (result << 17);
        }
        return result;
    }

    static final class PoolEntry implements Lockable {
        final int id;
        private final ReentrantLock lock = new ReentrantLock();

        PoolEntry(int id) {
            this.id = id;
        }

        @Override
        public void lock() {
            lock.lock();
        }

        @Override
        public boolean tryLock() {
            return lock.tryLock();
        }

        @Override
        public void unlock() {
            lock.unlock();
        }
    }
}

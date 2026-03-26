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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * JMH benchmark for {@link LockableConcurrentQueue} measuring throughput of
 * lock-and-poll / add-and-unlock cycles under varying concurrency levels.
 */
@Fork(3)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused")
public class LockableConcurrentQueueBenchmark {

    @Param({ "1", "4", "8" })
    int concurrency;

    @Param({ "16", "64" })
    int poolSize;

    private LockableConcurrentQueue<LockableEntry> queue;

    @Setup(Level.Iteration)
    public void setup() {
        queue = new LockableConcurrentQueue<>(LinkedList::new, concurrency);
        for (int i = 0; i < poolSize; i++) {
            LockableEntry entry = new LockableEntry();
            entry.lock();
            queue.addAndUnlock(entry);
        }
    }

    @Benchmark
    @Threads(1)
    public LockableEntry pollAndReturn_1thread() {
        return pollAndReturn();
    }

    @Benchmark
    @Threads(4)
    public LockableEntry pollAndReturn_4threads() {
        return pollAndReturn();
    }

    @Benchmark
    @Threads(8)
    public LockableEntry pollAndReturn_8threads() {
        return pollAndReturn();
    }

    private LockableEntry pollAndReturn() {
        LockableEntry entry = queue.lockAndPoll();
        if (entry != null) {
            queue.addAndUnlock(entry);
        }
        return entry;
    }

    @Benchmark
    @Threads(4)
    public void writerWorkload_4threads(Blackhole bh) {
        writerWorkload(bh);
    }

    @Benchmark
    @Threads(8)
    public void writerWorkload_8threads(Blackhole bh) {
        writerWorkload(bh);
    }

    @Benchmark
    @Threads(16)
    public void writerWorkload_16threads(Blackhole bh) {
        writerWorkload(bh);
    }

    private void writerWorkload(Blackhole bh) {
        LockableEntry entry = queue.lockAndPoll();
        if (entry != null) {
            bh.consume(simulateWork(entry));
            queue.addAndUnlock(entry);
        }
    }

    private static long simulateWork(LockableEntry entry) {
        long result = entry.hashCode();
        for (int i = 0; i < 20; i++) {
            result ^= (result << 13);
            result ^= (result >> 7);
            result ^= (result << 17);
        }
        return result;
    }

    static final class LockableEntry implements Lockable {
        private final ReentrantLock lock = new ReentrantLock();

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

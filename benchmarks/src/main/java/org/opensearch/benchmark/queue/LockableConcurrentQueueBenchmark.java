/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.queue;

import org.opensearch.common.queue.Lockable;
import org.opensearch.common.queue.LockableConcurrentQueue;
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
 * <p>
 * Includes two benchmark groups:
 * <ul>
 *   <li>{@code pollAndReturn} — minimal overhead: poll an entry and immediately return it.</li>
 *   <li>{@code writerWorkload} — simulates a writer pool: poll an entry, perform simulated
 *       document writes (CPU work), then return the entry. Models the composite writer
 *       checkout-write-return cycle.</li>
 * </ul>
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

    // ---- pollAndReturn: minimal overhead benchmarks ----

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

    // ---- writerWorkload: simulated writer pool benchmarks ----

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

    /**
     * Simulates a writer pool cycle: checkout an entry, perform CPU work
     * representing document indexing across multiple formats, then return it.
     */
    private void writerWorkload(Blackhole bh) {
        LockableEntry entry = queue.lockAndPoll();
        if (entry != null) {
            // Simulate document write work (field additions across formats)
            bh.consume(simulateDocumentWrite(entry));
            queue.addAndUnlock(entry);
        }
    }

    /**
     * Simulates the CPU cost of writing a document to multiple data formats.
     * Performs arithmetic work to prevent JIT elimination while keeping
     * the hold time realistic relative to a real addDoc call.
     */
    private static long simulateDocumentWrite(LockableEntry entry) {
        long result = entry.hashCode();
        // ~10 field additions across 2 formats
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

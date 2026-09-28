/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.util.concurrent.ReleasableLock;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Buffers document IDs and forwards row-ID deletes to the paired {@link Writer}. A read/write lock
 * allows concurrent recording while making {@link #deactivate()} an atomic drain.
 *
 * @opensearch.experimental
 */
public class DeleterImpl<T extends Writer<?>> implements Deleter {

    private static final long BYTES_PER_QUEUE_NODE = RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF
    );

    private final Writer<?> writer;
    private final long deleterGeneration;
    private final ReentrantReadWriteLock deleterLock;
    private final ReleasableLock deleterReadLock;
    private final ReleasableLock deleterWriteLock;
    private final Queue<String> bufferedDeletes = new ConcurrentLinkedQueue<>();
    /** Running footprint of {@link #bufferedDeletes}, maintained incrementally so reads never scan the queue. */
    private final AtomicLong bufferedDeletesRamBytesUsed = new AtomicLong();
    private volatile boolean active = true;

    public DeleterImpl(T writer) {
        this.writer = writer;
        this.deleterGeneration = writer.generation();

        this.deleterLock = new ReentrantReadWriteLock();
        this.deleterReadLock = new ReleasableLock(deleterLock.readLock());
        this.deleterWriteLock = new ReleasableLock(deleterLock.writeLock());
    }

    @Override
    public long generation() {
        return this.deleterGeneration;
    }

    @Override
    public boolean recordBufferedDeletes(String id) {
        try (ReleasableLock ignore = deleterReadLock.acquire()) {
            if (active == false) {
                throw new IllegalStateException("Cannot record a delete on a closed deleter.");
            }

            bufferedDeletes.add(id);
            bufferedDeletesRamBytesUsed.addAndGet(BYTES_PER_QUEUE_NODE + RamUsageEstimator.sizeOf(id));
            return true;
        }
    }

    @Override
    public void recordPositionalDelete(long rowId) {
        writer.recordPositionalDelete(rowId);
    }

    /**
     * Returns heap used by buffered IDs. Forwarded row-ID deletes are accounted by the paired writer.
     */
    @Override
    public long ramBytesUsed() {
        return bufferedDeletesRamBytesUsed.get();
    }

    @Override
    public void close() throws IOException {
        deactivate();
    }

    @Override
    public Queue<String> deactivate() {
        try (ReleasableLock ignore = deleterWriteLock.acquire()) {
            if (active == false) {
                return new ConcurrentLinkedQueue<>();
            }

            active = false;
            Queue<String> snapshot = new ConcurrentLinkedQueue<>(bufferedDeletes);
            bufferedDeletes.clear();
            bufferedDeletesRamBytesUsed.set(0L);
            return snapshot;
        }
    }

    @Override
    public boolean isActive() {
        return active;
    }
}

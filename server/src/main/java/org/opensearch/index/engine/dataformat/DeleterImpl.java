/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.apache.lucene.index.Term;
import org.opensearch.common.util.concurrent.ReleasableLock;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Lucene-based implementation of {@link Deleter} that delegates document deletion
 * to the paired {@link Writer}. Each instance shares the generation number
 * of its associated writer. Constructs the {@link Term} uid from the field name
 * and value provided in the {@link DeleteInput}.
 *
 * @opensearch.experimental
 */
public class DeleterImpl<T extends Writer<?>> implements Deleter {

    private final Writer<?> writer;
    private final long deleterGeneration;
    private final ReentrantReadWriteLock deleterLock;
    private final ReleasableLock deleterReadLock;
    private final ReleasableLock deleterWriteLock;
    private final Queue<String> bufferedDeletes = new ConcurrentLinkedQueue<>();
    private volatile boolean active = true;

    public DeleterImpl(T writer) {
        this.writer = writer;
        this.deleterGeneration = writer.generation();

        // TODO: Check if lock here is redundant??
        this.deleterLock = new ReentrantReadWriteLock();
        this.deleterReadLock = new ReleasableLock(deleterLock.readLock());
        this.deleterWriteLock = new ReleasableLock(deleterLock.writeLock());
    }

    @Override
    public long generation() {
        return this.deleterGeneration;
    }

    @Override
    public DeleteResult deleteDoc(DeleteInput deleteInput) throws IOException {
        try (ReleasableLock ignore = deleterReadLock.acquire()) {
            if (active == false) {
                return null;
            }

            return writer.deleteDocument(deleteInput);
        }
    }

    @Override
    public boolean recordBufferedDeletes(String id) {
        try (ReleasableLock ignore = deleterReadLock.acquire()) {
            if (active == false) {
                throw new IllegalStateException("Cannot record a delete on a closed deleter.");
            }

            bufferedDeletes.add(id);
            return true;
        }
    }

    @Override
    public void recordPositionalDelete(long rowId) {
        writer.recordPositionalDelete(rowId);
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
            return snapshot;
        }
    }

    @Override
    public boolean isActive() {
        return active;
    }
}

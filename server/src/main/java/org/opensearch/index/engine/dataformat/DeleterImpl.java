/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.apache.lucene.index.Term;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

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
    private final ReentrantLock lock;

    public DeleterImpl(T writer) {
        this.writer = writer;
        this.deleterGeneration = writer.generation();
        this.lock = new ReentrantLock();
    }

    @Override
    public long generation() {
        return this.deleterGeneration;
    }

    @Override
    public DeleteResult deleteDoc(DeleteInput deleteInput) throws IOException {
        return writer.deleteDocument(deleteInput);
    }

    @Override
    public void close() throws IOException {

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

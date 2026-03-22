/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.index.engine.dataformat.DataformatAwareLockableWriterPool;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests for {@link DataformatAwareLockableWriterPool} with composite-specific callbacks.
 */
public class DataformatAwareLockableWriterPoolTests extends OpenSearchTestCase {

    private CompositeIndexingExecutionEngine engine;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        engine = CompositeTestHelper.createStubEngine("lucene");
    }

    private DataformatAwareLockableWriterPool<CompositeWriter> createPool(int concurrency) {
        AtomicLong gen = new AtomicLong(0);
        DataformatAwareLockableWriterPool<CompositeWriter> pool = new DataformatAwareLockableWriterPool<>(
            ConcurrentLinkedQueue::new,
            concurrency
        );
        pool.initialize(() -> new CompositeWriter(engine, gen.getAndIncrement()));
        return pool;
    }

    public void testGetAndLockReturnsNewWriterWhenPoolEmpty() {
        DataformatAwareLockableWriterPool<CompositeWriter> pool = createPool(2);

        CompositeWriter writer = pool.getAndLock();
        assertNotNull(writer);
        assertEquals(0L, writer.getWriterGeneration());
        pool.releaseAndUnlock(writer);
    }

    public void testReleaseAndUnlockMakesWriterReusable() {
        DataformatAwareLockableWriterPool<CompositeWriter> pool = createPool(2);

        CompositeWriter writer = pool.getAndLock();
        pool.releaseAndUnlock(writer);

        CompositeWriter reused = pool.getAndLock();
        assertSame(writer, reused);
        pool.releaseAndUnlock(reused);
    }

    public void testGetAndLockThrowsWhenClosed() throws IOException {
        DataformatAwareLockableWriterPool<CompositeWriter> pool = createPool(2);
        pool.close();
        expectThrows(AlreadyClosedException.class, pool::getAndLock);
    }

    public void testCheckoutAllReturnsRegisteredWriters() {
        DataformatAwareLockableWriterPool<CompositeWriter> pool = createPool(4);

        CompositeWriter w1 = pool.getAndLock();
        CompositeWriter w2 = pool.getAndLock();
        pool.releaseAndUnlock(w1);
        pool.releaseAndUnlock(w2);

        List<CompositeWriter> checkedOut = pool.checkoutAll();
        assertEquals(2, checkedOut.size());
        for (CompositeWriter w : checkedOut) {
            assertFalse(w.isFlushPending());
        }
    }

    public void testCheckoutAllReturnsEmptyWhenNoWriters() {
        DataformatAwareLockableWriterPool<CompositeWriter> pool = createPool(2);

        List<CompositeWriter> checkedOut = pool.checkoutAll();
        assertTrue(checkedOut.isEmpty());
    }

    public void testCheckoutAllReturnsUnmodifiableList() {
        DataformatAwareLockableWriterPool<CompositeWriter> pool = createPool(2);

        CompositeWriter w = pool.getAndLock();
        pool.releaseAndUnlock(w);

        List<CompositeWriter> checkedOut = pool.checkoutAll();
        expectThrows(UnsupportedOperationException.class, () -> checkedOut.add(new CompositeWriter(engine, 99)));
    }

    public void testIsRegisteredReturnsTrueForPooledWriter() {
        DataformatAwareLockableWriterPool<CompositeWriter> pool = createPool(2);

        CompositeWriter writer = pool.getAndLock();
        assertTrue(pool.isRegistered(writer));
        pool.releaseAndUnlock(writer);
    }

    public void testIsRegisteredReturnsFalseForUnknownWriter() {
        DataformatAwareLockableWriterPool<CompositeWriter> pool = createPool(2);

        CompositeWriter unknown = new CompositeWriter(engine, 99);
        assertFalse(pool.isRegistered(unknown));
    }

    public void testCheckoutAllThrowsWhenClosed() throws IOException {
        DataformatAwareLockableWriterPool<CompositeWriter> pool = createPool(2);
        pool.close();
        expectThrows(AlreadyClosedException.class, pool::checkoutAll);
    }

    public void testIteratorReturnsSnapshotOfWriters() {
        DataformatAwareLockableWriterPool<CompositeWriter> pool = createPool(4);

        CompositeWriter w1 = pool.getAndLock();
        CompositeWriter w2 = pool.getAndLock();
        pool.releaseAndUnlock(w1);
        pool.releaseAndUnlock(w2);

        int count = 0;
        for (CompositeWriter w : pool) {
            assertNotNull(w);
            count++;
        }
        assertEquals(2, count);
    }
}

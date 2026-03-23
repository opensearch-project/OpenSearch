/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.queue;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tests for {@link LockablePool}.
 */
public class LockablePoolTests extends OpenSearchTestCase {

    /**
     * A simple lockable entry for testing.
     */
    static class LockableEntry implements Lockable {
        final String id;
        private final ReentrantLock delegate = new ReentrantLock();

        LockableEntry(String id) {
            this.id = id;
        }

        @Override
        public void lock() {
            delegate.lock();
        }

        @Override
        public boolean tryLock() {
            return delegate.tryLock();
        }

        @Override
        public void unlock() {
            delegate.unlock();
        }

        boolean isHeldByCurrentThread() {
            return delegate.isHeldByCurrentThread();
        }
    }

    private static int idCounter = 0;

    private LockablePool<LockableEntry> createPool() {
        return new LockablePool<>(() -> new LockableEntry("entry-" + idCounter++), LinkedList::new, 1);
    }

    public void testGetAndLockReturnsLockedItem() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry item = pool.getAndLock();
        assertNotNull(item);
        assertTrue(item.isHeldByCurrentThread());
        pool.releaseAndUnlock(item);
    }

    public void testReleaseAndUnlockAllowsReuse() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry first = pool.getAndLock();
        pool.releaseAndUnlock(first);

        LockableEntry second = pool.getAndLock();
        assertSame("Released item should be reused", first, second);
        pool.releaseAndUnlock(second);
    }

    public void testClosedPoolThrowsOnGetAndLock() throws IOException {
        LockablePool<LockableEntry> pool = createPool();
        pool.close();

        IllegalStateException ex = expectThrows(IllegalStateException.class, pool::getAndLock);
        assertEquals("LockablePool is already closed", ex.getMessage());
    }

    public void testCheckoutAllReturnsAllItems() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry a = pool.getAndLock();
        pool.releaseAndUnlock(a);
        LockableEntry b = pool.getAndLock();
        pool.releaseAndUnlock(b);

        // a and b may be the same item (reuse), so pool may have 1 or 2 items
        List<LockableEntry> all = pool.checkoutAll();
        assertFalse("checkoutAll should return at least one item", all.isEmpty());
    }

    public void testCheckoutAllOnEmptyPoolReturnsEmptyList() {
        LockablePool<LockableEntry> pool = createPool();
        List<LockableEntry> all = pool.checkoutAll();
        assertTrue("checkoutAll on empty pool should return empty list", all.isEmpty());
    }

    public void testCheckoutAllReturnsUnmodifiableList() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry item = pool.getAndLock();
        pool.releaseAndUnlock(item);

        List<LockableEntry> all = pool.checkoutAll();
        expectThrows(UnsupportedOperationException.class, () -> all.add(new LockableEntry("rogue")));
    }

    public void testIsRegisteredTrueForPoolItem() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry item = pool.getAndLock();
        assertTrue(pool.isRegistered(item));
        pool.releaseAndUnlock(item);
    }

    public void testIsRegisteredFalseForUnknownItem() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry stranger = new LockableEntry("stranger");
        assertFalse(pool.isRegistered(stranger));
    }

    public void testIteratorReturnsSnapshot() {
        LockablePool<LockableEntry> pool = createPool();
        LockableEntry a = pool.getAndLock();
        pool.releaseAndUnlock(a);

        int count = 0;
        for (LockableEntry item : pool) {
            assertNotNull(item);
            count++;
        }
        assertEquals(1, count);
    }

    public void testClosedPoolThrowsOnCheckoutAll() throws IOException {
        LockablePool<LockableEntry> pool = createPool();
        pool.close();

        IllegalStateException ex = expectThrows(IllegalStateException.class, pool::checkoutAll);
        assertEquals("LockablePool is already closed", ex.getMessage());
    }
}

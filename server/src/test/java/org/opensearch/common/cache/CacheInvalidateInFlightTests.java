/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A cache segment maps each key to a {@link java.util.concurrent.CompletableFuture}, and
 * {@code computeIfAbsent} installs that future in the segment map <em>before</em> the value is loaded,
 * so there is a window in which a key is in the segment map but its entry is not yet linked into the
 * LRU list. These tests cover what {@link Cache#invalidate(Object)} does with a key in that window.
 */
public class CacheInvalidateInFlightTests extends OpenSearchTestCase {

    private static final String TARGET = "target";
    private static final String STALL = "stall";

    /**
     * {@link Cache#keysSnapshot()} contains keys whose load is still in flight, and
     * {@link Cache#invalidate(Object)} on such a key parks the caller until the load completes: the
     * invalidation consumer calls {@code future.get()} on the incomplete future with no timeout.
     */
    public void testInvalidateBlocksOnInFlightLoad() throws Exception {
        final Cache<String, String> cache = CacheBuilder.<String, String>builder().build();
        final CountDownLatch loadStarted = new CountDownLatch(1);
        final CountDownLatch releaseLoad = new CountDownLatch(1);

        Thread loader = new Thread(() -> load(cache, loadStarted, releaseLoad), "loader");
        loader.start();
        assertTrue(loadStarted.await(10, TimeUnit.SECONDS));

        // the in-flight key is in the segment map, so keysSnapshot() returns it, even though nothing is
        // cached yet: it is not in the LRU list and is not counted
        assertEquals(List.of(TARGET), cache.keysSnapshot());
        assertEquals(List.of(), lruKeys(cache));
        assertEquals(0, cache.count());

        Thread invalidator = new Thread(() -> cache.invalidate(TARGET), "invalidator");
        invalidator.start();

        // invalidate() cannot return while the load is in flight
        assertBusy(() -> assertEquals(Thread.State.WAITING, invalidator.getState()));
        invalidator.join(500);
        assertTrue("invalidate() should still be blocked on the in-flight load", invalidator.isAlive());

        // it only unblocks once the loader finishes, however long that takes
        releaseLoad.countDown();
        invalidator.join(10_000);
        loader.join(10_000);
        assertFalse(invalidator.isAlive());
    }

    /**
     * When {@code invalidate(key)} deletes an entry before the thread that loaded it has promoted it,
     * {@code unlink()} finds {@code state == NEW}, returns false, and drops the removal notification.
     * The loading thread then links the entry into the LRU list even though its key is already gone from
     * the segment map, leaving an entry that is counted in {@code count()}/{@code weight()}, is
     * unreachable via {@code get()}, and can no longer be invalidated by key -- so its removal listener
     * never fires and the memory it accounts for is never given back.
     * <p>
     * The interleaving is forced rather than raced for: the weigher runs inside {@code linkAtHead()},
     * which gives the test thread a point where it holds the LRU lock, so the loading thread cannot
     * promote its entry until the test thread has finished invalidating it.
     */
    public void testInvalidateBeforePromoteLeavesUnreclaimableEntry() throws Exception {
        final AtomicReference<Cache<String, String>> cacheRef = new AtomicReference<>();
        final List<RemovalNotification<String, String>> removals = new CopyOnWriteArrayList<>();
        final CountDownLatch loadStarted = new CountDownLatch(1);
        final CountDownLatch releaseLoad = new CountDownLatch(1);
        final AtomicBoolean invalidatedUnderLruLock = new AtomicBoolean();

        final Cache<String, String> cache = CacheBuilder.<String, String>builder().weigher((key, value) -> {
            if (STALL.equals(key) && invalidatedUnderLruLock.compareAndSet(false, true)) {
                // holding the LRU lock: let the load finish and invalidate TARGET before its entry can
                // be promoted. invalidate() blocks until the load completes, then reenters the LRU lock.
                releaseLoad.countDown();
                cacheRef.get().invalidate(TARGET);
            }
            return 1L;
        }).removalListener(removals::add).build();
        cacheRef.set(cache);

        Thread loader = new Thread(() -> load(cache, loadStarted, releaseLoad), "loader");
        loader.start();
        assertTrue(loadStarted.await(10, TimeUnit.SECONDS));

        // TARGET's load is in flight; this put takes the LRU lock and runs the weigher above
        cache.put(STALL, "v");
        loader.join(10_000);
        assertFalse(loader.isAlive());
        assertTrue("the weigher never ran, so the interleaving under test did not happen", invalidatedUnderLruLock.get());

        assertEquals("TARGET should be gone from the segment map", List.of(STALL), cache.keysSnapshot());
        assertNull(cache.get(TARGET));
        assertEquals("TARGET should not be left in the LRU list", List.of(STALL), lruKeys(cache));
        assertEquals(1, cache.count());
        assertEquals(1L, cache.weight());
        assertEquals("the removal listener should have been told about TARGET", 1, removals.size());
        assertEquals(TARGET, removals.get(0).getKey());
        assertEquals(RemovalReason.INVALIDATED, removals.get(0).getRemovalReason());
    }

    private void load(Cache<String, String> cache, CountDownLatch loadStarted, CountDownLatch releaseLoad) {
        try {
            cache.computeIfAbsent(TARGET, k -> {
                loadStarted.countDown();
                releaseLoad.await();
                return "v";
            });
        } catch (ExecutionException e) {
            throw new AssertionError(e);
        }
    }

    /** The keys in the LRU list, head first. */
    private List<String> lruKeys(Cache<String, String> cache) {
        List<String> keys = new ArrayList<>();
        for (String key : cache.keys()) {
            keys.add(key);
        }
        return keys;
    }
}

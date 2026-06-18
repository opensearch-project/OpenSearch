/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.shuffle;

import org.opensearch.analytics.exec.shuffle.ShuffleBufferManager.AdmitResult;
import org.opensearch.analytics.spi.ShuffleBufferExceededException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ShuffleBufferManagerTests extends OpenSearchTestCase {

    public void testGetOrCreateReturnsStableBuffer() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        ShuffleBufferManager.ShuffleBuffer a = mgr.getOrCreateBuffer("q1", 0, 0);
        ShuffleBufferManager.ShuffleBuffer b = mgr.getOrCreateBuffer("q1", 0, 0);
        assertSame("same key must return same buffer", a, b);

        ShuffleBufferManager.ShuffleBuffer c = mgr.getOrCreateBuffer("q1", 1, 0);
        assertNotSame("different stageId must yield a different buffer", a, c);
    }

    /**
     * Codex review P1: buffers must be keyed by partitionIndex too, otherwise two partitions of the
     * same stage that happen to land on the same worker node would share a buffer and leak rows +
     * {@code isLast} markers across partitions. This test pins the correct key shape and the
     * resulting isolation.
     */
    public void testDifferentPartitionsSameStageHaveDistinctBuffers() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        ShuffleBufferManager.ShuffleBuffer p0 = mgr.getOrCreateBuffer("q1", 0, 0);
        ShuffleBufferManager.ShuffleBuffer p1 = mgr.getOrCreateBuffer("q1", 0, 1);
        assertNotSame("partition 0 and partition 1 must not share a buffer", p0, p1);

        // Feed data into each partition and confirm isolation — p0 sees only its own bytes.
        p0.addData("left", new byte[] { 10, 20 });
        p1.addData("left", new byte[] { 99 });
        assertEquals(2L, p0.getCurrentBytes());
        assertEquals(1L, p1.getCurrentBytes());
    }

    public void testRemoveBufferDeletesIt() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.getOrCreateBuffer("q1", 0, 0);
        assertNotNull(mgr.getBuffer("q1", 0, 0));
        mgr.removeBuffer("q1", 0, 0);
        assertNull(mgr.getBuffer("q1", 0, 0));
    }

    /**
     * clearForQuery must remove EVERY buffer for the query (all stages + partitions on this node) so
     * the on-heap payload becomes collectible at query end, while leaving OTHER queries' buffers
     * intact. This is the cross-query leak fix — without it buffers live for the JVM's lifetime.
     */
    public void testClearForQueryRemovesAllQueryBuffersOnly() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        // q1: two stages, multiple partitions (a cascade puts >1 worker stage on a node).
        mgr.getOrCreateBuffer("q1", 2, 0);
        mgr.getOrCreateBuffer("q1", 2, 1);
        mgr.getOrCreateBuffer("q1", 4, 0);
        // q2: a different query whose buffers must survive q1's cleanup.
        mgr.getOrCreateBuffer("q2", 2, 0);

        int removed = mgr.clearForQuery("q1");
        assertEquals("clearForQuery must remove all 3 of q1's buffers", 3, removed);
        assertNull(mgr.getBuffer("q1", 2, 0));
        assertNull(mgr.getBuffer("q1", 2, 1));
        assertNull(mgr.getBuffer("q1", 4, 0));
        assertNotNull("a different query's buffer must NOT be cleared", mgr.getBuffer("q2", 2, 0));
    }

    /** clearForQuery is idempotent and a no-op (0 removed) for a query with no buffers — the common
     *  non-shuffle / already-cleaned case, plus the cancellation backstop firing after normal cleanup. */
    public void testClearForQueryIdempotentAndNoOpWhenAbsent() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        assertEquals("no buffers → 0 removed", 0, mgr.clearForQuery("nonexistent"));
        assertEquals("null queryId → 0 removed, no NPE", 0, mgr.clearForQuery(null));

        mgr.getOrCreateBuffer("q1", 0, 0);
        assertEquals(1, mgr.clearForQuery("q1"));
        assertEquals("second clear is a no-op", 0, mgr.clearForQuery("q1"));
    }

    /** Prefix matching must not be fooled by a query id that is a PREFIX of another (q1 vs q10):
     *  the key separator ':' makes "q1:" not a prefix of "q10:". */
    public void testClearForQueryPrefixIsExactPerQueryId() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.getOrCreateBuffer("q1", 0, 0);
        mgr.getOrCreateBuffer("q10", 0, 0);
        assertEquals("only q1's buffer, not q10's", 1, mgr.clearForQuery("q1"));
        assertNull(mgr.getBuffer("q1", 0, 0));
        assertNotNull("q10 must be untouched by clearForQuery(q1)", mgr.getBuffer("q10", 0, 0));
    }

    /**
     * After clearForQuery (cancel/abort), a late producer RPC racing in via getOrCreateBuffer must
     * NOT repopulate the map — it gets an unstored throwaway buffer instead, so the cleared query
     * stays cleared and the late write lands on a collectible object. Without the tombstone this
     * recreated buffer would leak (no consumer to drain/remove it).
     */
    public void testGetOrCreateAfterClearDoesNotRepopulate() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.getOrCreateBuffer("q1", 2, 0);
        assertEquals(1, mgr.clearForQuery("q1"));
        // Late producer RPC for the cancelled query:
        ShuffleBufferManager.ShuffleBuffer late = mgr.getOrCreateBuffer("q1", 2, 0);
        assertNotNull("getOrCreate must still return a usable (throwaway) buffer, never null", late);
        assertNull("the throwaway must NOT be stored in the map", mgr.getBuffer("q1", 2, 0));
        // A different (live) query is unaffected by q1's tombstone.
        ShuffleBufferManager.ShuffleBuffer live = mgr.getOrCreateBuffer("q2", 0, 0);
        assertSame("non-aborted query buffers are stored normally", live, mgr.getBuffer("q2", 0, 0));
    }

    /**
     * Hard per-query ceiling: when a SINGLE query's footprint would exceed the per-query budget,
     * tryAdmit throws a NON-retryable ShuffleBufferExceededException (not REJECT_RETRY). This is the
     * q17-OOM fix — a query that can't fit even on an idle node fails fast instead of buffering its
     * whole shuffle on-heap until OOM.
     */
    public void testTryAdmitThrowsWhenQueryExceedsPerQueryBudget() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setBudgets(/* node */ 100, /* perQuery */ 10);
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit("q1", 0, 0, "left", new byte[5]));
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit("q1", 0, 0, "left", new byte[5]));
        assertEquals(10L, mgr.getTotalBytes());
        // Next chunk pushes q1's own footprint to 11 > perQuery=10 → hard, non-retryable throw.
        ShuffleBufferExceededException ex = expectThrows(
            ShuffleBufferExceededException.class,
            () -> mgr.tryAdmit("q1", 0, 0, "right", new byte[1])
        );
        assertEquals(11L, ex.observedBytes());
        assertEquals(10L, ex.limitBytes());
        assertEquals("rejected chunk must not be reserved", 10L, mgr.getTotalBytes());
    }

    /**
     * Soft node-contention: when the NODE total would exceed the node budget but the query still
     * fits its per-query share, tryAdmit returns REJECT_RETRY (retryable — room frees when other
     * queries finish), NOT a throw.
     */
    public void testTryAdmitRejectsRetryableOnNodeContention() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        // node budget 10, per-query budget 10 (each query may use the whole node if alone).
        mgr.setBudgets(/* node */ 10, /* perQuery */ 10);
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit("q1", 0, 0, "left", new byte[6]));
        // q2 still fits its own 10-byte share, but node total 6+5=11 > 10 → retryable reject.
        assertEquals(AdmitResult.REJECT_RETRY, mgr.tryAdmit("q2", 1, 0, "left", new byte[5]));
        assertEquals("rejected chunk not reserved", 6L, mgr.getTotalBytes());
        // When q1 finishes and releases, q2's retry now fits.
        mgr.clearForQuery("q1");
        assertEquals(0L, mgr.getTotalBytes());
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit("q2", 1, 0, "left", new byte[5]));
    }

    /** Budget disabled (Long.MAX_VALUE) → tryAdmit always accepts, never throws/rejects. */
    public void testNoBudgetAlwaysAccepts() {
        ShuffleBufferManager mgr = new ShuffleBufferManager(); // defaults to Long.MAX_VALUE budgets
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit("q1", 0, 0, "left", new byte[10_000]));
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit("q1", 0, 0, "right", new byte[10_000]));
        assertEquals(20_000L, mgr.getTotalBytes());
    }

    /** removeBuffer releases that buffer's reserved bytes back to the node + per-query budgets. */
    public void testRemoveBufferReleasesBytes() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setBudgets(100, 100);
        mgr.tryAdmit("q1", 0, 0, "left", new byte[30]);
        mgr.tryAdmit("q1", 1, 0, "left", new byte[20]);
        assertEquals(50L, mgr.getTotalBytes());
        mgr.removeBuffer("q1", 0, 0); // frees 30
        assertEquals(20L, mgr.getTotalBytes());
        mgr.removeBuffer("q1", 1, 0); // frees 20
        assertEquals(0L, mgr.getTotalBytes());
    }

    /**
     * BLOCKER (codex review: tombstoned-admit-budget-leak): once a query is tombstoned by
     * clearForQuery, a late producer RPC must NOT reserve budget. The buffer it gets is an unstored
     * throwaway that no removeBuffer/clearForQuery will ever release, so reserving bytes for it would
     * leak the node budget permanently. tryAdmit must drop the payload (still ACCEPTED — the producer
     * shouldn't retry a cancelled query) and leave totalBytes untouched.
     */
    public void testTryAdmitOnTombstonedQueryReservesNoBytes() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setBudgets(1_000, 1_000);
        mgr.tryAdmit("q1", 0, 0, "left", new byte[100]);
        assertEquals(100L, mgr.getTotalBytes());
        // Cancel/terminal clears q1 and tombstones it; budget fully reclaimed.
        mgr.clearForQuery("q1");
        assertEquals(0L, mgr.getTotalBytes());
        // Late producer RPC racing in after the clear: accepted (don't make it retry) but NOT reserved.
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit("q1", 0, 0, "left", new byte[100]));
        assertEquals("tombstoned admit must not reserve budget", 0L, mgr.getTotalBytes());
        assertNull("tombstoned admit must not repopulate the map", mgr.getBuffer("q1", 0, 0));
    }

    /**
     * BLOCKER (codex review: shuffle-budget-double-release): removeBuffer (per-buffer, on normal
     * drain) and clearForQuery (whole-query, on terminal) must compose without double-subtracting.
     * Drain one buffer via removeBuffer, then clearForQuery the rest — totalBytes must land at exactly
     * 0, never undercount (which would silently shrink the effective budget for later queries).
     */
    public void testRemoveBufferThenClearForQueryDoesNotDoubleRelease() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setBudgets(1_000, 1_000);
        mgr.tryAdmit("q1", 0, 0, "left", new byte[100]);
        mgr.tryAdmit("q1", 1, 0, "left", new byte[200]);
        assertEquals(300L, mgr.getTotalBytes());
        mgr.removeBuffer("q1", 0, 0); // drains + releases the 100-byte buffer
        assertEquals(200L, mgr.getTotalBytes());
        // clearForQuery must release only the REMAINING 200 (already net of the removeBuffer above).
        mgr.clearForQuery("q1");
        assertEquals("no double-release: budget must return to exactly 0", 0L, mgr.getTotalBytes());
        // Budget fully reclaimed → a fresh query can now use the whole node.
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit("q2", 0, 0, "left", new byte[1_000]));
    }

    /**
     * BLOCKER (codex review round 2: tombstoned-admit-budget-leak via tombstone eviction). With the
     * prior COUNT-based FIFO cap, a still-in-window tombstone could be evicted by N other clears, and
     * a late producer admit for that query would then reserve bytes that no release reclaims. The fix
     * is TTL-based tombstones (provably outlive the producer retry window). This test pins the
     * invariant directly: a tombstone is LIVE right after clearForQuery, and an admit while tombstoned
     * reserves NO bytes — independent of how many OTHER queries are cleared in between (the case that
     * used to evict the tombstone). We clear many other queries to simulate the eviction pressure.
     */
    public void testTombstoneSurvivesManyOtherClearsAndBlocksReservation() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setBudgets(Long.MAX_VALUE, Long.MAX_VALUE);
        mgr.tryAdmit("victim", 0, 0, "left", new byte[100]);
        assertEquals(100L, mgr.getTotalBytes());
        mgr.clearForQuery("victim");
        assertEquals(0L, mgr.getTotalBytes());
        // Simulate heavy churn: clear far more than the old FIFO cap would have retained.
        for (int i = 0; i < 10_000; i++) {
            mgr.clearForQuery("other-" + i);
        }
        // The victim's tombstone must STILL be live (TTL is 120s; this loop is sub-second), so a late
        // producer RPC for the victim reserves nothing and does not repopulate the map.
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit("victim", 0, 0, "left", new byte[100]));
        assertEquals("late admit on a tombstoned query must reserve no bytes", 0L, mgr.getTotalBytes());
        assertEquals("and reserve nothing against the per-query footprint", 0L, mgr.getQueryBytes("victim"));
        assertNull("and must not repopulate the buffer map", mgr.getBuffer("victim", 0, 0));
    }

    /**
     * Concurrency stress for the accounting BLOCKERs: many producers admit while terminal hooks
     * removeBuffer / clearForQuery concurrently. After all activity quiesces and every query is
     * cleared, totalBytes AND every per-query footprint must be back to 0 — no leaked reservations
     * (orphaned-admission, double-release, tombstoned-admit). Worker threads capture any throwable and
     * fail the test; all threads must terminate. Generous budget so admission never rejects — we're
     * testing counter integrity under racing mutations, not the gates.
     */
    public void testConcurrentAdmitClearKeepsCountersConsistent() throws Exception {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setBudgets(Long.MAX_VALUE, Long.MAX_VALUE);
        int queries = 8;
        int producersPerQuery = 6;
        Thread[] threads = new Thread[queries * (producersPerQuery + 1)];
        CountDownLatch start = new CountDownLatch(1);
        CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<>();
        Thread.UncaughtExceptionHandler ueh = (thread, ex) -> failures.add(ex);
        int t = 0;
        for (int qi = 0; qi < queries; qi++) {
            String q = "q" + qi;
            for (int p = 0; p < producersPerQuery; p++) {
                int stage = p;
                threads[t] = new Thread(() -> {
                    awaitQuietly(start);
                    for (int i = 0; i < 50; i++) {
                        // Stage doubles as partition spread; some admits race the clear below.
                        mgr.tryAdmit(q, stage, i % 3, (i % 2 == 0) ? "left" : "right", new byte[16]);
                        if (i % 7 == 0) {
                            mgr.removeBuffer(q, stage, i % 3);
                        }
                    }
                });
                threads[t].setUncaughtExceptionHandler(ueh);
                t++;
            }
            // One terminal thread per query that clears mid-flight (the cancel/abort race).
            threads[t] = new Thread(() -> {
                awaitQuietly(start);
                mgr.clearForQuery(q);
            });
            threads[t].setUncaughtExceptionHandler(ueh);
            t++;
        }
        for (Thread th : threads) {
            th.start();
        }
        start.countDown();
        for (Thread th : threads) {
            th.join(30_000);
            assertFalse("worker thread did not terminate within 30s", th.isAlive());
        }
        assertTrue("worker threads threw: " + failures, failures.isEmpty());
        // Final terminal sweep for every query (the real coordinator broadcasts this on every
        // terminal); after it, all reservations — node total AND per-query — must be released.
        for (int qi = 0; qi < queries; qi++) {
            mgr.clearForQuery("q" + qi);
            assertEquals("per-query footprint must be fully reclaimed", 0L, mgr.getQueryBytes("q" + qi));
        }
        assertEquals("no leaked reservations after all queries cleared", 0L, mgr.getTotalBytes());
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void testAwaitReadyCompletesWhenBothSidesDone() throws Exception {
        ShuffleBufferManager.ShuffleBuffer buffer = new ShuffleBufferManager.ShuffleBuffer();
        buffer.setExpectedSenders(2, 1);
        // Two left senders, one right sender. Mark them done from another thread, then await.
        CountDownLatch startGate = new CountDownLatch(1);
        Thread senderThread = new Thread(() -> {
            try {
                startGate.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            buffer.senderDone("left");
            buffer.senderDone("left");
            buffer.senderDone("right");
        });
        senderThread.start();
        startGate.countDown();
        assertTrue("awaitReady must complete after all senders signal done", buffer.awaitReady(5_000));
        senderThread.join(1_000);
    }

    public void testAwaitReadyTimesOut() throws Exception {
        ShuffleBufferManager.ShuffleBuffer buffer = new ShuffleBufferManager.ShuffleBuffer();
        buffer.setExpectedSenders(1, 1);
        // Mark left done but leave right pending — short timeout must return false.
        buffer.senderDone("left");
        assertFalse("awaitReady must time out when right side never completes", buffer.awaitReady(100));
    }

    public void testCompletionTriggeredEvenIfSendersFinishBeforeExpectedSet() throws Exception {
        // Edge case: senders may signal done before setExpectedSenders is called. The implementation
        // re-checks after set, so the buffer must transition ready.
        ShuffleBufferManager.ShuffleBuffer buffer = new ShuffleBufferManager.ShuffleBuffer();
        buffer.senderDone("left");
        buffer.senderDone("right");
        buffer.setExpectedSenders(1, 1);
        assertTrue(buffer.awaitReady(1_000));
    }
}

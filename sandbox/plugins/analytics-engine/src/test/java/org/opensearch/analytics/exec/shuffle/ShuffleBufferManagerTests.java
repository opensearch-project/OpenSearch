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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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

    // ---------------------------------------------------------------------------------------------
    // Disk-spill (analytics.mpp.shuffle.spill.*) — when enabled, a per-query budget breach spills the
    // oldest buffered chunks to disk instead of failing fast, preserving the buffer-all consumer
    // contract (getLeftData/getRightData return the FULL chunk list in ARRIVAL order). When disabled,
    // behavior is byte-identical to the pre-spill fail-fast path (the tests above stay green).
    // ---------------------------------------------------------------------------------------------

    /** Distinct, recognizable chunk content so order assertions are meaningful. */
    private static byte[] chunk(int marker, int size) {
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) {
            b[i] = (byte) marker;
        }
        return b;
    }

    /**
     * Counts {@code .spill} files directly under {@code dir} (0 if the dir is absent). Filtering on
     * the {@code .spill} suffix is deliberate: the test framework's {@code ExtrasFS} mockfile injects
     * random {@code extraN} files/dirs into directories to catch code that assumes empty dirs, so a
     * bare "directory is empty" check would spuriously fail. We assert on OUR files only.
     */
    private static long countSpillFiles(Path dir) throws Exception {
        if (!Files.isDirectory(dir)) {
            return 0L;
        }
        try (var s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().endsWith(".spill")).count();
        }
    }

    /**
     * Spill enabled: feeding chunks that exceed the per-query budget does NOT throw — the oldest
     * resident chunks spill to disk. The consumer's getLeftData() returns ALL chunks in ARRIVAL order
     * (spilled-then-tail), currentBytes/totalBytes reflect ONLY the in-memory-resident bytes (spilled
     * bytes released), and clearForQuery deletes the spill file (no disk leak).
     */
    public void testSpillEnabledExceedsBudgetNoThrowAndDrainsInArrivalOrder() throws Exception {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        // Per-query budget 100 bytes; node budget generous so only the per-query gate triggers spill.
        mgr.setBudgets(/* node */ 10_000, /* perQuery */ 100);
        mgr.setSpillConfig(true, spillDir, /* maxBytes */ 1_000_000);

        // Feed five 40-byte chunks (200 bytes total) into the left side of one partition. The budget
        // is 100 bytes, so the oldest chunks spill to keep resident bounded, and NONE of these admits
        // throws.
        for (int i = 0; i < 5; i++) {
            assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit("q1", 0, 0, "left", chunk(i, 40)));
        }

        // Resident on-heap must be bounded by the per-query budget — never the full 200 bytes.
        assertTrue("resident bytes must be bounded by the budget, was " + mgr.getTotalBytes(), mgr.getTotalBytes() <= 100);
        assertTrue("per-query resident bounded too", mgr.getQueryBytes("q1") <= 100);
        // The remainder lives on disk.
        assertTrue("some bytes must have spilled to disk", mgr.getSpilledTotalBytes() > 0);

        // The consumer drains the FULL set of 5 chunks in ARRIVAL order (spilled chunks first, then
        // the in-memory tail).
        ShuffleBufferManager.ShuffleBuffer buffer = mgr.getBuffer("q1", 0, 0);
        assertNotNull(buffer);
        List<byte[]> drained = buffer.getLeftData();
        assertEquals("all 5 chunks must be drained (spilled + tail)", 5, drained.size());
        for (int i = 0; i < 5; i++) {
            assertEquals("chunk " + i + " size", 40, drained.get(i).length);
            assertEquals("chunk " + i + " must be in arrival order (marker " + i + ")", (byte) i, drained.get(i)[0]);
        }

        // A spill file must exist under the per-query subdir before cleanup.
        Path queryDir = spillDir.resolve("q1");
        assertTrue("spill subdir must exist", Files.isDirectory(queryDir));
        assertTrue("at least one .spill file must exist", countSpillFiles(queryDir) > 0);

        // clearForQuery must delete the spill file(s) AND the per-query subdir (no disk leak), and
        // release all budget + disk accounting.
        mgr.clearForQuery("q1");
        assertEquals("no .spill file may survive the terminal", 0L, countSpillFiles(queryDir));
        assertEquals("node budget fully reclaimed", 0L, mgr.getTotalBytes());
        assertEquals("disk accounting fully reclaimed", 0L, mgr.getSpilledTotalBytes());
    }

    /**
     * Spill enabled: removeBuffer (the normal per-buffer drain terminal) must also delete that
     * buffer's spill file and release its on-disk bytes — not just clearForQuery.
     */
    public void testSpillEnabledRemoveBufferDeletesSpillFile() throws Exception {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setBudgets(10_000, 100);
        mgr.setSpillConfig(true, spillDir, 1_000_000);
        for (int i = 0; i < 5; i++) {
            mgr.tryAdmit("q1", 7, 3, "right", chunk(i, 40));
        }
        Path queryDir = spillDir.resolve("q1");
        assertTrue(Files.isDirectory(queryDir));
        long spilledBefore = mgr.getSpilledTotalBytes();
        assertTrue("expected some spill", spilledBefore > 0);
        assertTrue("a .spill file must exist before removeBuffer", countSpillFiles(queryDir) > 0);

        mgr.removeBuffer("q1", 7, 3);
        assertNull(mgr.getBuffer("q1", 7, 3));
        assertEquals("removeBuffer must delete the buffer's .spill file", 0L, countSpillFiles(queryDir));
        assertEquals("on-disk bytes released after removeBuffer", 0L, mgr.getSpilledTotalBytes());
        assertEquals("node budget released after removeBuffer", 0L, mgr.getTotalBytes());
    }

    /**
     * Spill enabled but the chunks fit within the per-query budget: nothing spills, the spill subdir
     * is never created, and getLeftData returns the plain in-memory list (the fast no-spill path).
     */
    public void testSpillEnabledNoSpillWhenUnderBudget() {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setBudgets(10_000, 1_000);
        mgr.setSpillConfig(true, spillDir, 1_000_000);
        mgr.tryAdmit("q1", 0, 0, "left", chunk(1, 40));
        mgr.tryAdmit("q1", 0, 0, "left", chunk(2, 40));
        assertEquals(80L, mgr.getTotalBytes());
        assertEquals("nothing spilled when under budget", 0L, mgr.getSpilledTotalBytes());
        assertFalse("no spill subdir created when nothing spills", Files.exists(spillDir.resolve("q1")));
        List<byte[]> drained = mgr.getBuffer("q1", 0, 0).getLeftData();
        assertEquals(2, drained.size());
    }

    /**
     * Disk ceiling: with spill enabled but a tiny spill.max_bytes, exceeding the per-query budget
     * forces a spill that breaches the disk ceiling → terminal ShuffleBufferExceededException (the
     * disk-ceiling variant), NOT a silent run. limitBytes() reflects the disk ceiling.
     */
    public void testSpillEnabledDiskCeilingThrows() throws Exception {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setBudgets(10_000, 100);
        // Disk ceiling of 60 bytes. Each spilled 50-byte chunk costs 54 bytes on disk (50 payload + a
        // 4-byte frame header), so the first spill (54 <= 60) fits and the second (108 > 60) breaches.
        mgr.setSpillConfig(true, spillDir, /* maxBytes */ 60);
        // Fill the budget: 100 bytes resident (chunks 0,1 of 50 each). Under budget so far.
        mgr.tryAdmit("q1", 0, 0, "left", chunk(0, 50));
        mgr.tryAdmit("q1", 0, 0, "left", chunk(1, 50));
        assertEquals(100L, mgr.getTotalBytes());
        // Next chunk forces eviction of one chunk to disk → spilled total 54 (fits the 60 ceiling).
        mgr.tryAdmit("q1", 0, 0, "left", chunk(2, 50)); // spills chunk 0 (54 <= 60 ceiling)
        ShuffleBufferExceededException ex = expectThrows(
            ShuffleBufferExceededException.class,
            () -> mgr.tryAdmit("q1", 0, 0, "left", chunk(3, 50)) // would spill a 2nd chunk → 108 > 60
        );
        assertEquals("disk-ceiling exception names the spill.max_bytes limit", 60L, ex.limitBytes());
        // Terminal cleanup (as the coordinator does on failure) closes + deletes the open spill file.
        mgr.clearForQuery("q1");
        assertEquals("no .spill file may survive the terminal", 0L, countSpillFiles(spillDir.resolve("q1")));
    }

    /**
     * Spill DISABLED (the default): a per-query budget breach still throws the fail-fast
     * ShuffleBufferExceededException — byte-identical to the pre-spill behavior. Even with a spill
     * directory configured, setSpillConfig(false, ...) keeps spill off. No spill file is created.
     */
    public void testSpillDisabledStillFailsFast() {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setBudgets(10_000, 100);
        mgr.setSpillConfig(false, spillDir, 1_000_000); // disabled
        mgr.tryAdmit("q1", 0, 0, "left", chunk(0, 60));
        ShuffleBufferExceededException ex = expectThrows(
            ShuffleBufferExceededException.class,
            () -> mgr.tryAdmit("q1", 0, 0, "left", chunk(1, 60)) // 120 > 100, fail fast
        );
        assertEquals(120L, ex.observedBytes());
        assertEquals(100L, ex.limitBytes());
        assertEquals("rejected chunk not reserved", 60L, mgr.getTotalBytes());
        assertEquals("nothing spilled when disabled", 0L, mgr.getSpilledTotalBytes());
        assertFalse("no spill dir created when disabled", Files.exists(spillDir.resolve("q1")));
    }

    /**
     * LAZY drain (the property that makes spill actually remove the heap ceiling): {@code drainLeft()}
     * streams a spilled partition's chunks back ONE AT A TIME from disk, then the in-memory tail — it
     * must NOT re-materialize the whole partition into a List. This test feeds many chunks that far
     * exceed the per-query budget (so most are on disk), drains via the lazy iterator, and asserts the
     * full arrival sequence is reproduced AND the on-heap resident set stayed bounded throughout.
     */
    public void testSpillEnabledLazyDrainStreamsInArrivalOrder() throws Exception {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        // Tight per-query budget so almost everything spills; the lazy iterator must still reproduce
        // the full arrival order without ever holding all chunks resident.
        mgr.setBudgets(/* node */ 1_000_000, /* perQuery */ 100);
        mgr.setSpillConfig(true, spillDir, /* maxBytes */ 10_000_000);

        int n = 50; // 50 chunks × 40 bytes = 2000 bytes; budget 100 → ~48 chunks on disk
        for (int i = 0; i < n; i++) {
            assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit("q1", 0, 0, "left", chunk(i, 40)));
        }
        assertTrue("resident must stay bounded by the budget", mgr.getTotalBytes() <= 100);
        assertTrue("the bulk must be on disk", mgr.getSpilledTotalBytes() > 0);

        ShuffleBufferManager.ShuffleBuffer buffer = mgr.getBuffer("q1", 0, 0);
        assertNotNull(buffer);

        // Drain via the LAZY iterator — pull one chunk at a time, asserting arrival order. We never
        // build a full List from it, mirroring the consumer's chunk-at-a-time pump.
        int seen = 0;
        try (var it = buffer.drainLeft()) {
            while (it.hasNext()) {
                byte[] c = it.next();
                assertEquals("chunk " + seen + " size", 40, c.length);
                assertEquals("chunk " + seen + " must be in arrival order", (byte) seen, c[0]);
                seen++;
            }
        }
        assertEquals("lazy drain must yield every chunk exactly once", n, seen);

        // Terminal cleanup releases disk + budget and deletes the spill file.
        mgr.clearForQuery("q1");
        assertEquals("no .spill file may survive the terminal", 0L, countSpillFiles(spillDir.resolve("q1")));
        assertEquals("disk accounting fully reclaimed", 0L, mgr.getSpilledTotalBytes());
    }

    /**
     * Lazy drain on a NON-spilled side returns the plain in-memory chunks (fast path) and its
     * {@code close()} is a harmless no-op — exercises the {@code spill == null} branch of the lazy
     * drain so both branches are covered.
     */
    public void testLazyDrainNonSpilledSide() throws Exception {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setBudgets(10_000, 1_000); // generous → no spill
        mgr.tryAdmit("q1", 0, 0, "left", chunk(1, 40));
        mgr.tryAdmit("q1", 0, 0, "left", chunk(2, 40));
        assertEquals("nothing spilled", 0L, mgr.getSpilledTotalBytes());

        ShuffleBufferManager.ShuffleBuffer buffer = mgr.getBuffer("q1", 0, 0);
        int seen = 0;
        try (var it = buffer.drainLeft()) {
            while (it.hasNext()) {
                assertEquals((byte) (seen + 1), it.next()[0]);
                seen++;
            }
        }
        assertEquals(2, seen);
    }

    /**
     * Cross-partition spill (codex round-3 BLOCKER #1): the per-query budget is query-WIDE, but a
     * single partition buffer may not hold enough resident bytes to cover a breach when the footprint
     * is spread across sibling partitions of the same stage. When partition 1's first chunk arrives and
     * partition 0 already holds the whole budget, the manager must spill partition 0 (a SAME-stage
     * sibling) to admit partition 1 — NOT fail-fast or REJECT_RETRY. Both partitions stay drainable.
     */
    public void testSpillEnabledCrossPartitionSpillsSibling() throws Exception {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        // node==perQuery==100 (the production default: setBudgets(budget,budget)). Generous disk.
        mgr.setBudgets(100, 100);
        mgr.setSpillConfig(true, spillDir, /* maxBytes */ 1_000_000);

        // Partition 0 of stage 0 fills the whole per-query budget (resident=100).
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit("q1", 0, 0, "left", chunk(0, 50)));
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit("q1", 0, 0, "left", chunk(1, 50)));
        assertEquals(100L, mgr.getTotalBytes());

        // Partition 1's first chunk: query-wide would be 140 > 100. Pre-fix this REJECT_RETRY'd to
        // exhaustion (p1 had nothing of its own to spill); now the manager spills sibling p0.
        assertEquals("cross-partition spill must admit, not reject", AdmitResult.ACCEPTED, mgr.tryAdmit("q1", 0, 1, "left", chunk(2, 40)));
        assertTrue("sibling partition 0 must have spilled to disk", mgr.getSpilledTotalBytes() > 0);
        assertTrue("resident stays bounded by the per-query budget", mgr.getTotalBytes() <= 100);

        // Both partitions still drain their full chunk set in arrival order.
        List<byte[]> p0 = mgr.getBuffer("q1", 0, 0).getLeftData();
        assertEquals("partition 0 keeps both chunks (spilled + tail)", 2, p0.size());
        assertEquals((byte) 0, p0.get(0)[0]);
        assertEquals((byte) 1, p0.get(1)[0]);
        List<byte[]> p1 = mgr.getBuffer("q1", 0, 1).getLeftData();
        assertEquals("partition 1 keeps its chunk", 1, p1.size());
        assertEquals((byte) 2, p1.get(0)[0]);

        mgr.clearForQuery("q1");
        assertEquals("disk accounting fully reclaimed", 0L, mgr.getSpilledTotalBytes());
    }

    /**
     * Cross-partition spill must NOT cross stage boundaries: a buffer of a DIFFERENT (possibly already-
     * draining) stage is never spilled to make room for an incoming chunk. Only same-(query,stage)
     * siblings are eligible. (codex round-3 BLOCKER #1 safety boundary.)
     */
    public void testSpillEnabledCrossPartitionDoesNotSpillOtherStage() throws Exception {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setBudgets(100, 100);
        mgr.setSpillConfig(true, spillDir, 1_000_000);

        // Stage 9 partition 0 holds 60 bytes resident (a different stage — must be left untouched).
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit("q1", 9, 0, "left", chunk(7, 60)));
        // Stage 0 partition 0 holds 40 → query-wide resident now 100 (== budget).
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit("q1", 0, 0, "left", chunk(0, 40)));
        assertEquals(100L, mgr.getTotalBytes());

        // Stage 0 partition 1's chunk: must spill same-stage sibling (stage 0 p0), NOT stage 9.
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit("q1", 0, 1, "left", chunk(1, 40)));

        // Stage 9's buffer kept all its bytes in memory (no spill file for it).
        assertEquals("other-stage buffer must stay fully resident", 60L, mgr.getBuffer("q1", 9, 0).getCurrentBytes());
        Path queryDir = spillDir.resolve("q1");
        // The only spill file(s) must belong to stage 0 (key prefix "0-"), never stage 9 ("9-").
        try (var s = Files.list(queryDir)) {
            assertTrue("no stage-9 spill file may exist", s.map(p -> p.getFileName().toString()).noneMatch(n -> n.startsWith("9-")));
        }
        mgr.clearForQuery("q1");
    }

    /**
     * Tombstone-race spill leak (codex round-3 BLOCKER #2): when getOrCreateBuffer's computeIfAbsent
     * returns a PRE-EXISTING buffer that already SPILLED, and the query is then found tombstoned, the
     * discard path must release the buffer's on-disk spill bytes (and delete its files) — not just drop
     * the map entry. A lock-free buffers.remove would leak spilledTotalBytes permanently. This drives
     * the discard helper directly (the live branch only fires in a narrow concurrent window).
     */
    public void testDiscardRacedBufferReleasesSpillBytes() throws Exception {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setBudgets(100, 100);
        mgr.setSpillConfig(true, spillDir, 1_000_000);

        // Fill + overflow partition 0 so it spills to disk.
        mgr.tryAdmit("q1", 0, 0, "left", chunk(0, 50));
        mgr.tryAdmit("q1", 0, 0, "left", chunk(1, 50));
        mgr.tryAdmit("q1", 0, 0, "left", chunk(2, 50)); // forces a spill of the oldest chunk
        assertTrue("precondition: something spilled", mgr.getSpilledTotalBytes() > 0);
        ShuffleBufferManager.ShuffleBuffer spilled = mgr.getBuffer("q1", 0, 0);
        assertNotNull(spilled);
        Path queryDir = spillDir.resolve("q1");
        assertTrue("a spill file must exist before discard", countSpillFiles(queryDir) > 0);

        // Simulate the raced discard of the existing (already-spilled) buffer — getOrCreateBuffer
        // calls this after its post-insert tombstone re-check fires.
        mgr.discardRacedBuffer("q1", "q1:0:0", spilled);

        assertNull("buffer must be removed from the map", mgr.getBuffer("q1", 0, 0));
        assertEquals("on-disk spill bytes must be released (no leak)", 0L, mgr.getSpilledTotalBytes());
        assertEquals("resident bytes released too", 0L, mgr.getTotalBytes());
        assertEquals("spill file deleted", 0L, countSpillFiles(queryDir));
    }

    /**
     * Discard must be conditional: if the map entry for the key is NOT our buffer (a racing producer
     * replaced it), discardRacedBuffer must leave the other buffer and its accounting alone.
     */
    public void testDiscardRacedBufferLeavesUnrelatedBuffer() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setBudgets(1_000, 1_000);
        mgr.tryAdmit("q1", 0, 0, "left", chunk(0, 40)); // the LIVE buffer at q1:0:0
        long before = mgr.getTotalBytes();

        // A throwaway buffer that was never the map entry for q1:0:0.
        ShuffleBufferManager.ShuffleBuffer stranger = new ShuffleBufferManager.ShuffleBuffer();
        mgr.discardRacedBuffer("q1", "q1:0:0", stranger);

        assertNotNull("the real buffer must remain", mgr.getBuffer("q1", 0, 0));
        assertEquals("accounting must be untouched", before, mgr.getTotalBytes());
    }

    /**
     * Cross-partition spill must NOT evict from a sibling that has begun DRAINING (codex round-4
     * BLOCKER #1): a drain may have already snapshotted the sibling's in-memory tail / opened its
     * spill file, so spilling it would drop/duplicate rows or NPE. Here partition 0 fills the budget
     * then begins draining; partition 1's incoming chunk must NOT spill partition 0 (it stays
     * resident + fully drainable), even though that means partition 1's admit runs over budget.
     */
    public void testCrossPartitionSpillSkipsDrainingSibling() throws Exception {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setBudgets(100, 100);
        mgr.setSpillConfig(true, spillDir, 1_000_000);

        // Partition 0 fills the whole budget, then a consumer begins draining it.
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit("q1", 0, 0, "left", chunk(0, 50)));
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit("q1", 0, 0, "left", chunk(1, 50)));
        ShuffleBufferManager.ShuffleBuffer p0 = mgr.getBuffer("q1", 0, 0);
        try (var it = p0.drainLeft()) {            // marks p0 draining (beginDrain under admitLock)
            assertTrue(p0.isDraining());

            // Partition 1's chunk arrives while p0 is draining. Pre-fix this would spill sibling p0;
            // now p0 is skipped. With node==perQuery and p0 holding the whole budget, nothing is
            // spillable, so the admit is REJECT_RETRY (the safe fallback — the producer retries and
            // succeeds once p0's drain completes and removeBuffer frees its budget). The CRITICAL
            // invariant is that p0 was NOT spilled and its in-flight drain stays intact.
            assertEquals(AdmitResult.REJECT_RETRY, mgr.tryAdmit("q1", 0, 1, "left", chunk(2, 40)));
            assertEquals("a draining sibling must not be spilled", 0L, mgr.getSpilledTotalBytes());

            // p0's in-flight drain still yields BOTH of its chunks intact, in order.
            int seen = 0;
            while (it.hasNext()) {
                assertEquals((byte) seen, it.next()[0]);
                seen++;
            }
            assertEquals("draining sibling delivers all its chunks despite the concurrent admit", 2, seen);
        }
        mgr.clearForQuery("q1");
    }

    /**
     * Disk-byte accounting must not leak when a spill-file delete FAILS then the per-query dir sweep
     * later removes the file (codex round-4 SHOULD-FIX #2). We can't easily force a delete IOException
     * in a unit test, so this asserts the happy-path invariant the orphan tracker preserves: after a
     * normal terminal, spilledTotalBytes returns to 0 and the orphan map is empty (no path left
     * charged). The orphan-release path itself is exercised by deleteQuerySpillDir on every clear.
     */
    public void testSpillByteAccountingFullyReclaimedOnClear() throws Exception {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setBudgets(100, 100);
        mgr.setSpillConfig(true, spillDir, 1_000_000);
        mgr.tryAdmit("q1", 0, 0, "left", chunk(0, 50));
        mgr.tryAdmit("q1", 0, 0, "left", chunk(1, 50));
        mgr.tryAdmit("q1", 0, 0, "left", chunk(2, 50)); // spills
        assertTrue(mgr.getSpilledTotalBytes() > 0);
        mgr.clearForQuery("q1");
        assertEquals("disk accounting fully reclaimed after clear", 0L, mgr.getSpilledTotalBytes());
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.shuffle;

import org.opensearch.analytics.exec.shuffle.ShuffleBufferManager.AdmitResult;
import org.opensearch.analytics.spi.CloseableIterator;
import org.opensearch.analytics.spi.ShuffleSlots;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The MATERIALIZED shuffle mode: a barrier before the drain (pipelining off) plus disk, which is the
 * combination that makes a bounded in-flight window safe.
 *
 * <p>The distinction these pin is scheduling, not storage. Pipelined means the consumer drains
 * concurrently with its producers; materialized means it waits for all of them. Whether bytes touch a
 * disk is orthogonal — and it is precisely what the barrier has to buy, because a barrier without a
 * durable home pays for lost overlap and gets nothing back: peak residency is still the whole partition
 * (that was the pre-streaming behaviour), backpressure can still fail a query, and there is no
 * re-readable copy.
 *
 * <p>So the two properties asserted here are:
 * <ul>
 *   <li>a full window is relieved by SPILLING, so admission never returns a retryable reject and the
 *       "rejected N times, query failed" terminal state cannot arise;</li>
 *   <li>the spilled prefix is read back ONE FRAME AT A TIME, so an over-budget partition is not
 *       re-materialized on the heap at drain — which would hand the peak straight back.</li>
 * </ul>
 */
public class ShuffleMaterializedModeTests extends OpenSearchTestCase {

    private static final String Q = "q-materialized";
    private static final String LEFT = ShuffleSlots.LEFT;

    /** A chunk whose first byte carries {@code marker}, so arrival order is checkable after a spill. */
    private static byte[] chunk(int marker, int size) {
        byte[] b = new byte[size];
        b[0] = (byte) marker;
        return b;
    }

    /** Manager with spill on and pipelining off — the materialized mode. */
    private ShuffleBufferManager materializedManager(Path spillDir) {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setPipelinedEnabled(false);
        mgr.setSpillConfig(true, spillDir, /* maxBytes */ 100L * 1024 * 1024);
        return mgr;
    }

    private static List<byte[]> drainAll(ShuffleBufferManager.ShuffleBuffer buf, String slot) {
        List<byte[]> out = new ArrayList<>();
        try (CloseableIterator<byte[]> it = buf.drain(slot, 30_000)) {
            while (it.hasNext()) {
                out.add(it.next());
            }
        }
        return out;
    }

    /**
     * The headline property of this mode: with a window configured and spill available, EVERY admit is
     * accepted. A retryable reject is what lets backpressure fail a query (the sender's attempt budget is
     * finite), so a mode that never rejects cannot fail for backpressure at all — the ceiling becomes
     * disk, which is a real resource limit rather than a timing artefact.
     */
    public void testFullWindowSpillsRatherThanRejecting() {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = materializedManager(spillDir);
        mgr.setStreamWindowBytes(400);
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);

        for (int i = 0; i < 40; i++) {
            assertEquals(
                "admit " + i + " must be accepted: a full window is relieved by disk, never by a retry",
                AdmitResult.ACCEPTED,
                mgr.tryAdmit(Q, 0, 0, LEFT, chunk(i, 100))
            );
        }
        assertEquals("no admission may have been rejected", 0L, buf.getRejectedCount());
        assertTrue("the overflow must be on disk, was " + mgr.getSpilledTotalBytes(), mgr.getSpilledTotalBytes() > 0);
        assertTrue("heap residency must stay within the window, was " + buf.queuedBytes(LEFT), buf.queuedBytes(LEFT) <= 400);

        // Terminal (as every query does, on success or failure): the spill file is closed and deleted and
        // its disk bytes returned, so relief cannot leak disk across queries.
        mgr.clearForQuery(Q);
        assertEquals("disk accounting fully reclaimed", 0L, mgr.getSpilledTotalBytes());
    }

    /** Window relief must preserve ARRIVAL order across the spill/resident boundary: the drain reads the
     *  spill file (oldest chunks, in write order) and only then the resident queue. */
    public void testWindowSpillPreservesArrivalOrder() {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = materializedManager(spillDir);
        mgr.setStreamWindowBytes(400);
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);

        int n = 40;
        for (int i = 0; i < n; i++) {
            assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit(Q, 0, 0, LEFT, chunk(i, 100)));
        }
        buf.senderDone(LEFT);

        List<byte[]> drained = drainAll(buf, LEFT);
        assertEquals("every chunk must be delivered exactly once", n, drained.size());
        for (int i = 0; i < n; i++) {
            assertEquals("chunk " + i + " out of arrival order", (byte) i, drained.get(i)[0]);
            assertEquals(100, drained.get(i).length);
        }
        mgr.clearForQuery(Q);
        assertEquals("disk accounting fully reclaimed after the drain", 0L, mgr.getSpilledTotalBytes());
    }

    /**
     * The guard that keeps relief safe: once a consumer owns the queue, eviction would take from the head
     * the consumer is reading, so the window falls back to the retryable reject. This is the control arm
     * for the test above — without it, "spill relieves the window" would silently also apply to a live
     * drain and drop or duplicate rows.
     */
    public void testFullWindowStillRejectsOnceTheDrainOwnsTheQueue() {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = new ShuffleBufferManager(); // pipelined (default) => drain starts at once
        mgr.setSpillConfig(true, spillDir, 100L * 1024 * 1024);
        mgr.setStreamWindowBytes(100);
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);

        try (CloseableIterator<byte[]> ignored = buf.drain(LEFT, 100)) {
            assertTrue("the drain must own the queue for this arm", buf.isDraining());
            assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit(Q, 0, 0, LEFT, chunk(0, 60)));
            assertEquals(
                "a draining buffer must not be spilled from under its consumer — reject instead",
                AdmitResult.REJECT_RETRY,
                mgr.tryAdmit(Q, 0, 0, LEFT, chunk(1, 60))
            );
            assertEquals("nothing may have been written to disk", 0L, mgr.getSpilledTotalBytes());
        }
    }

    /** A chunk larger than the window still goes into an EMPTY slot: relief cannot split a chunk, so the
     *  one-item escape has to survive the spill path (otherwise that chunk is rejected forever). */
    public void testChunkLargerThanTheWindowStillAdmittedIntoAnEmptySlot() {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = materializedManager(spillDir);
        mgr.setStreamWindowBytes(100);
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);

        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit(Q, 0, 0, LEFT, chunk(0, 500)));
        // Queue non-empty and already over the window: relief spills the 500-byte chunk, so the next one
        // is admitted rather than rejected.
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit(Q, 0, 0, LEFT, chunk(1, 500)));
        assertTrue("the first chunk must have gone to disk", mgr.getSpilledTotalBytes() > 0);

        buf.senderDone(LEFT);
        List<byte[]> drained = drainAll(buf, LEFT);
        assertEquals(2, drained.size());
        assertEquals((byte) 0, drained.get(0)[0]);
        assertEquals((byte) 1, drained.get(1)[0]);
    }

    /**
     * The spilled phase must be read frame-at-a-time. Pulling ONE chunk from a partition with many
     * spilled chunks must read exactly one frame — the whole-file read that used to sit here meant a
     * partition spilled to disk and was then fully re-loaded onto the heap at drain, so the peak that
     * spill exists to remove came right back.
     */
    public void testSpilledPhaseIsReadOneFrameAtATime() {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = materializedManager(spillDir);
        mgr.setStreamWindowBytes(200);
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);

        int n = 30;
        for (int i = 0; i < n; i++) {
            assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit(Q, 0, 0, LEFT, chunk(i, 100)));
        }
        buf.senderDone(LEFT);
        assertTrue("this test needs a spilled prefix", mgr.getSpilledTotalBytes() > 0);

        try (CloseableIterator<byte[]> it = buf.drain(LEFT, 30_000)) {
            assertTrue(it.hasNext());
            assertEquals("the oldest chunk must come first", (byte) 0, it.next()[0]);
            assertEquals("one consumed chunk must cost exactly one frame read", 1L, buf.spilledFramesRead(LEFT));

            assertTrue(it.hasNext());
            assertEquals((byte) 1, it.next()[0]);
            assertEquals(2L, buf.spilledFramesRead(LEFT));
        }
    }

    /**
     * The derived default: a materialized, spill-capable buffer with NO configured window is bounded by
     * {@link ShuffleBufferManager#MATERIALIZED_WINDOW_DEFAULT_BYTES} rather than by the per-query budget
     * (80% of heap in production, which is the footprint that pins old-gen for the rest of the query).
     */
    public void testMaterializedModeBoundsResidencyWithoutAConfiguredWindow() {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = materializedManager(spillDir);
        assertEquals("no window configured for this test", Long.MAX_VALUE, mgr.getStreamWindowBytes());
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);

        int chunkSize = 2 * 1024 * 1024;
        int n = (int) (ShuffleBufferManager.MATERIALIZED_WINDOW_DEFAULT_BYTES / chunkSize) + 2;
        for (int i = 0; i < n; i++) {
            assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit(Q, 0, 0, LEFT, chunk(i, chunkSize)));
        }
        assertTrue(
            "residency must be bounded by the derived window, was " + buf.queuedBytes(LEFT),
            buf.queuedBytes(LEFT) <= ShuffleBufferManager.MATERIALIZED_WINDOW_DEFAULT_BYTES
        );
        assertTrue("the overflow must be on disk", mgr.getSpilledTotalBytes() > 0);
        mgr.clearForQuery(Q);
        assertEquals("disk accounting fully reclaimed", 0L, mgr.getSpilledTotalBytes());
    }

    /**
     * ...and the derived default is materialized-ONLY. Under pipelining the same window would be relieved
     * by a retryable reject, which is the measured failure mode (the smaller the window, the more queries
     * failed), so pipelined buffers stay unbounded unless an operator opts in.
     */
    public void testDerivedWindowDoesNotApplyUnderPipelining() {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = new ShuffleBufferManager(); // pipelined by default
        mgr.setSpillConfig(true, spillDir, 100L * 1024 * 1024);
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);

        int chunkSize = 2 * 1024 * 1024;
        int n = (int) (ShuffleBufferManager.MATERIALIZED_WINDOW_DEFAULT_BYTES / chunkSize) + 2;
        for (int i = 0; i < n; i++) {
            assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit(Q, 0, 0, LEFT, chunk(i, chunkSize)));
        }
        assertEquals("no window applies under pipelining, so nothing may spill", 0L, mgr.getSpilledTotalBytes());
        assertEquals("everything stays resident", (long) n * chunkSize, buf.queuedBytes(LEFT));
    }

    /**
     * Materialized with spill DISABLED must IGNORE the window rather than enforce it.
     *
     * <p>Enforcing it there is a guaranteed failure, not backpressure: no consumer drains until every
     * sender is done, so a rejected producer retries against a queue that cannot shrink and fails when
     * its attempt budget runs out — for any partition bigger than the window. Same rule as the
     * empty-slot escape: a window with no admissible item is a deadlock. Accumulating to the per-query
     * budget is worse for residency but cannot invent a failure.
     */
    public void testMaterializedWithoutSpillIgnoresTheWindowRatherThanFailing() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setPipelinedEnabled(false);
        mgr.setStreamWindowBytes(100);
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);

        for (int i = 0; i < 5; i++) {
            assertEquals(
                "no relief path exists, so the window must not reject",
                AdmitResult.ACCEPTED,
                mgr.tryAdmit(Q, 0, 0, LEFT, chunk(i, 60))
            );
        }
        assertEquals("everything stays resident (bounded by the per-query budget, not the window)", 300L, buf.queuedBytes(LEFT));
    }

    /** A PIPELINED buffer keeps enforcing the window with a reject when spill is unavailable: there a
     *  drain is already running, so a reject genuinely can be relieved. */
    public void testPipelinedWithoutSpillStillRejects() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setStreamWindowBytes(100);
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);

        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit(Q, 0, 0, LEFT, chunk(0, 60)));
        assertEquals(AdmitResult.REJECT_RETRY, mgr.tryAdmit(Q, 0, 0, LEFT, chunk(1, 60)));
    }

    /**
     * The barrier must NOT be bounded by the caller's PER-CHUNK timeout.
     *
     * <p>The two bound different things: the caller's value is how long to wait for one chunk while
     * producers are already shipping (kept small so an unfed partition fails in a minute, not five), while
     * the barrier waits out the whole producer phase, which is minutes at scale. Conflating them is not a
     * tuning mistake but a correctness one — every materialized stage dies at the barrier reporting
     * {@code 0/N senders} however healthy the data path is. Measured on the sf=100 cluster: q9's shuffle
     * had already written 3.8 GB per node to disk when the 60s per-chunk value expired and failed the
     * query.
     *
     * <p>Pinned with a 1 ms per-chunk timeout and a sender that finishes later: if the barrier used the
     * argument, this could not pass.
     */
    public void testBarrierIsNotBoundedByThePerChunkTimeout() throws Exception {
        Path spillDir = createTempDir();
        ShuffleBufferManager mgr = materializedManager(spillDir);
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        buf.setExpectedSenders(1, -1);
        assertEquals(AdmitResult.ACCEPTED, mgr.tryAdmit(Q, 0, 0, LEFT, chunk(0, 16)));

        Thread producer = new Thread(() -> {
            try {
                Thread.sleep(300); // far longer than the per-chunk timeout below
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            buf.senderDone(LEFT);
        }, "test-slow-producer");
        producer.setDaemon(true);
        producer.start();

        // 1 ms per-chunk timeout. The barrier has to use its own, much larger bound, so this must return
        // the chunk rather than throw "timed out ... waiting for shuffle producers".
        try (CloseableIterator<byte[]> it = buf.drain(LEFT, /* perChunkMillis */ 1)) {
            assertTrue("the barrier must wait for the producer, not for the per-chunk timeout", it.hasNext());
            assertEquals((byte) 0, it.next()[0]);
        }
        producer.join(TimeUnit.SECONDS.toMillis(10));
    }

    /** The mode is per worker stage, and an instruction can only NARROW to materialized — the node-level
     *  kill switch must not be overridable by a plan. */
    public void testUseMaterializedModeIsOneWay() {
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        ShuffleBufferManager.ShuffleBuffer buf = mgr.getOrCreateBuffer(Q, 0, 0);
        assertTrue("buffers inherit the node default", buf.isPipelined());

        buf.useMaterializedMode();
        assertFalse("the stage's decision must take effect", buf.isPipelined());

        buf.useMaterializedMode();
        assertFalse("idempotent", buf.isPipelined());
    }
}

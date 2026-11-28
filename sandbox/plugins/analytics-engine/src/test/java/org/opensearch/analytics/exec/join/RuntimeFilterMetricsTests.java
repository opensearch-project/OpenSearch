/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tests for the runtime-filter funnel counters.
 *
 * <p>Worth testing directly despite being a counter class, for a reason this feature has already
 * demonstrated twice: every stage of a runtime filter may silently decline, so the counters are the only
 * thing that distinguishes "the filter fired and bought nothing" from "the filter never fired". A counter
 * that is declared but never recorded, or recorded with the wrong unit, reads exactly like a real
 * failure — and both of those bugs shipped into this branch before being caught in review rather than by
 * a test.
 */
public class RuntimeFilterMetricsTests extends OpenSearchTestCase {

    public void testEveryCounterStartsAtZeroAndIsPresentInASnapshot() {
        // A snapshot must name every counter, not only the ones touched so far. A missing key would force
        // every reader to distinguish absent from zero, and the endpoint's whole purpose is to show a zero
        // where a stage of the funnel declined.
        Map<RuntimeFilterMetrics.Counter, Long> snapshot = new RuntimeFilterMetrics().snapshot();

        assertEquals(RuntimeFilterMetrics.Counter.values().length, snapshot.size());
        for (RuntimeFilterMetrics.Counter counter : RuntimeFilterMetrics.Counter.values()) {
            assertEquals(counter + " starts at zero", Long.valueOf(0L), snapshot.get(counter));
        }
    }

    public void testRecordAccumulatesAndIncrementAddsOne() {
        RuntimeFilterMetrics metrics = new RuntimeFilterMetrics();

        metrics.record(RuntimeFilterMetrics.Counter.SHUFFLE_PLANNED, 3L);
        metrics.record(RuntimeFilterMetrics.Counter.SHUFFLE_PLANNED, 2L);
        metrics.increment(RuntimeFilterMetrics.Counter.SHUFFLE_PLANNED);

        assertEquals(6L, (long) metrics.snapshot().get(RuntimeFilterMetrics.Counter.SHUFFLE_PLANNED));
    }

    public void testCountersDoNotBleedIntoEachOther() {
        // The funnel is only readable if each step is independent: a narrowing between two steps is the
        // signal, so a write to one counter leaking into another would fabricate or hide exactly that.
        RuntimeFilterMetrics metrics = new RuntimeFilterMetrics();
        metrics.record(RuntimeFilterMetrics.Counter.PAYLOAD_ATTACHED, 7L);

        Map<RuntimeFilterMetrics.Counter, Long> snapshot = metrics.snapshot();
        assertEquals(7L, (long) snapshot.get(RuntimeFilterMetrics.Counter.PAYLOAD_ATTACHED));
        for (RuntimeFilterMetrics.Counter other : RuntimeFilterMetrics.Counter.values()) {
            if (other != RuntimeFilterMetrics.Counter.PAYLOAD_ATTACHED) {
                assertEquals(other + " must be untouched", Long.valueOf(0L), snapshot.get(other));
            }
        }
    }

    public void testRecordingZeroLeavesTheCounterAtZero() {
        // The attach paths record whatever they attached, including nothing. Recording zero must stay
        // distinguishable from recording one, because "attached 0 filters" is a legitimate outcome that a
        // reader has to be able to tell apart from "this code path never ran".
        RuntimeFilterMetrics metrics = new RuntimeFilterMetrics();
        metrics.record(RuntimeFilterMetrics.Counter.BROADCAST_CAN_MATCH_ATTACHED, 0L);

        assertEquals(0L, (long) metrics.snapshot().get(RuntimeFilterMetrics.Counter.BROADCAST_CAN_MATCH_ATTACHED));
    }

    public void testASnapshotDoesNotChangeAfterwards() {
        // Callers serialise a snapshot into a REST response, so it must not move under them mid-write.
        RuntimeFilterMetrics metrics = new RuntimeFilterMetrics();
        metrics.record(RuntimeFilterMetrics.Counter.PRE_PASS_RUN, 1L);
        Map<RuntimeFilterMetrics.Counter, Long> taken = metrics.snapshot();

        metrics.record(RuntimeFilterMetrics.Counter.PRE_PASS_RUN, 10L);

        assertEquals("the snapshot is a copy, not a view", 1L, (long) taken.get(RuntimeFilterMetrics.Counter.PRE_PASS_RUN));
        assertEquals("while the live counter moved on", 11L, (long) metrics.snapshot().get(RuntimeFilterMetrics.Counter.PRE_PASS_RUN));
    }

    public void testConcurrentRecordsAreNotLost() throws Exception {
        // Pre-passes settle on shard response handler threads, so several of them record concurrently. A
        // non-atomic counter would lose increments here and under-report the funnel exactly when the
        // feature is doing the most work.
        RuntimeFilterMetrics metrics = new RuntimeFilterMetrics();
        int threads = 8;
        int perThread = 1000;
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> workers = new ArrayList<>(threads);
        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                try {
                    assertTrue(start.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < perThread; i++) {
                    metrics.increment(RuntimeFilterMetrics.Counter.PAYLOAD_ATTACHED);
                    metrics.record(RuntimeFilterMetrics.Counter.PAYLOAD_BYTES, 2L);
                }
            });
            workers.add(worker);
            worker.start();
        }
        start.countDown();
        for (Thread worker : workers) {
            worker.join(30_000);
            assertFalse("worker did not finish", worker.isAlive());
        }

        Map<RuntimeFilterMetrics.Counter, Long> snapshot = metrics.snapshot();
        assertEquals((long) threads * perThread, (long) snapshot.get(RuntimeFilterMetrics.Counter.PAYLOAD_ATTACHED));
        assertEquals(2L * threads * perThread, (long) snapshot.get(RuntimeFilterMetrics.Counter.PAYLOAD_BYTES));
    }
}

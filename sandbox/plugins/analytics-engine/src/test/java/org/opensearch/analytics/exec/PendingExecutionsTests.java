/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * Tests {@link PendingExecutions}' permit-based admission: at most {@code permits} pieces of work run
 * concurrently, the rest queue and drain as permits are released. This is the mechanism behind the
 * per-node {@code max_concurrent_shard_requests_per_node} throttle.
 */
public class PendingExecutionsTests extends OpenSearchTestCase {

    public void testRunsImmediatelyWhenPermitsAvailable() {
        PendingExecutions pending = new PendingExecutions(2);
        List<Integer> ran = new ArrayList<>();
        pending.tryRun(taker(ran, 0));
        pending.tryRun(taker(ran, 1));
        assertEquals("both run while permits are available", List.of(0, 1), ran);
    }

    public void testQueuesBeyondLimitAndDrainsOnFinish() {
        PendingExecutions pending = new PendingExecutions(2);
        List<Integer> ran = new ArrayList<>();

        // Two permits; nothing here calls finishAndRunNext, so both hold their permits.
        pending.tryRun(taker(ran, 0));
        pending.tryRun(taker(ran, 1));
        // Third exceeds the limit → queued, not yet run.
        pending.tryRun(taker(ran, 2));
        assertEquals("third is queued, not run", List.of(0, 1), ran);

        // Release one permit → the queued work drains.
        pending.finishAndRunNext();
        assertEquals("queued work runs once a permit frees", List.of(0, 1, 2), ran);
    }

    public void testFinishWithEmptyQueueIsNoOp() {
        PendingExecutions pending = new PendingExecutions(1);
        List<Integer> ran = new ArrayList<>();
        pending.tryRun(taker(ran, 0));
        // No work queued behind it — releasing the permit must not run anything or throw.
        pending.finishAndRunNext();
        assertEquals(List.of(0), ran);
        // A subsequent submission still runs (permit is available again).
        pending.tryRun(taker(ran, 1));
        assertEquals(List.of(0, 1), ran);
    }

    public void testLimitOfOneSerializes() {
        PendingExecutions pending = new PendingExecutions(1);
        List<Integer> ran = new ArrayList<>();
        pending.tryRun(taker(ran, 0));
        pending.tryRun(taker(ran, 1)); // queued behind the held permit
        pending.tryRun(taker(ran, 2)); // queued
        assertEquals("only the first runs at limit 1", List.of(0), ran);

        pending.finishAndRunNext();
        assertEquals(List.of(0, 1), ran);
        pending.finishAndRunNext();
        assertEquals(List.of(0, 1, 2), ran);
    }

    // ── tryRun: admitted work that turns out to have nothing to do ──

    /** Declining with nothing waiting releases the permit rather than holding it forever. */
    public void testDeclineWithEmptyQueueReleasesThePermit() {
        PendingExecutions pending = new PendingExecutions(1);
        List<Integer> ran = new ArrayList<>();

        pending.tryRun(() -> false);
        pending.tryRun(taker(ran, 0));

        assertEquals("the permit was free for the next submission", List.of(0), ran);
    }

    /**
     * A long run of declines, then a taker, checks two things: every decline forwards its permit (or at
     * limit 1 the taker never runs), and draining them doesn't grow the stack per decline.
     */
    public void testALongRunOfDeclinesForwardsThePermitWithoutGrowingTheStack() {
        PendingExecutions pending = new PendingExecutions(1);
        List<Integer> ran = new ArrayList<>();
        List<Integer> depths = new ArrayList<>();

        pending.tryRun(taker(ran, 0));   // holds the only permit while everything below queues
        for (int i = 0; i < 500; i++) {
            pending.tryRun(() -> {
                depths.add(new Exception().getStackTrace().length);
                return false;
            });
        }
        pending.tryRun(taker(ran, 1));
        assertEquals("only the permit holder has run", List.of(0), ran);

        pending.finishAndRunNext();

        assertEquals("500 declines forwarded the permit to the taker behind them", List.of(0, 1), ran);
        assertEquals("every queued decliner ran", 500, depths.size());
        int first = depths.get(0);
        int last = depths.get(depths.size() - 1);
        assertTrue("stack depth must not grow across the run (first=" + first + ", last=" + last + ")", Math.abs(last - first) <= 2);
    }

    /** Work that records that it ran and takes the permit — the ordinary, non-declining case. */
    private static BooleanSupplier taker(List<Integer> ran, int id) {
        return () -> {
            ran.add(id);
            return true;
        };
    }
}

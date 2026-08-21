/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.concurrency.limit;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.TimeUnit;

import com.netflix.concurrency.limits.limit.AbstractLimit;

public class BurstAwareLimitTests extends OpenSearchTestCase {

    /**
     * A delegate whose limit never changes, regardless of onSample inputs.
     * Used to isolate BurstAwareLimit's window state machine from Vegas's adaptive behavior.
     */
    private static class FixedLimit extends AbstractLimit {
        FixedLimit(int limit) {
            super(limit);
        }

        @Override
        protected int _update(long s, long rtt, int inflight, boolean dropped) {
            return getLimit();
        }
    }

    /**
     * A delegate that can be driven to a new limit externally, simulating algorithm updates.
     */
    private static class MutableLimit extends AbstractLimit {
        MutableLimit(int initial) {
            super(initial);
        }

        void updateTo(int newLimit) {
            setLimit(newLimit);
        }

        @Override
        protected int _update(long s, long rtt, int inflight, boolean dropped) {
            return getLimit();
        }
    }

    private static final long RTT = TimeUnit.MILLISECONDS.toNanos(10);

    public void testGetLimitReturnsDelegatePlusBurst() {
        FixedLimit base = new FixedLimit(20);
        BurstAwareLimit burst = new BurstAwareLimit(base, 5, 3, 3);
        assertEquals(25, burst.getLimit());
        assertEquals(20, base.getLimit());
    }

    public void testZeroBurstCapacityIsTransparent() {
        FixedLimit base = new FixedLimit(20);
        BurstAwareLimit burst = new BurstAwareLimit(base, 0, 3, 3);
        assertEquals(base.getLimit(), burst.getLimit());
    }

    public void testBurstLimitFollowsAlgorithmUpdates() {
        MutableLimit base = new MutableLimit(20);
        BurstAwareLimit burst = new BurstAwareLimit(base, 5, 2, 3);
        assertEquals(25, burst.getLimit());

        // Algorithm grows the base — burst follows.
        base.updateTo(30);
        assertEquals(35, burst.getLimit()); // 30 + 5

        // Close the burst window — 2 consecutive samples with inflight >= base (30).
        burst.onSample(0, RTT, 30, false); // counter=1
        assertEquals("burst must not close before threshold", 35, burst.getLimit());
        burst.onSample(0, RTT, 31, false); // counter=2 → closes
        assertEquals("burst must be closed after N consecutive over-base samples", 30, burst.getLimit());

        // Algorithm changes base while window is closed — propagated without burst.
        base.updateTo(25);
        assertEquals(25, burst.getLimit());

        // After threshold quiet samples the window reopens.
        burst.onSample(0, RTT, 10, false); // counter=1
        burst.onSample(0, RTT, 12, false); // counter=2
        assertEquals("burst window must remain closed before recovery threshold", 25, burst.getLimit());
        burst.onSample(0, RTT, 14, false); // counter=3 → reopens
        assertEquals(30, burst.getLimit()); // 25 + 5
    }

    public void testBurstWindowClosesAfterConsecutiveOverBaseSamples() {
        FixedLimit base = new FixedLimit(20);
        BurstAwareLimit burst = new BurstAwareLimit(base, 5, 3, 5);
        assertEquals(25, burst.getLimit());

        // inflight >= base (20) — these are over-base samples.
        burst.onSample(0, RTT, 20, false); // counter=1
        assertEquals("window must not close after 1 of 3", 25, burst.getLimit());
        burst.onSample(0, RTT, 22, false); // counter=2
        assertEquals("window must not close after 2 of 3", 25, burst.getLimit());
        burst.onSample(0, RTT, 24, false); // counter=3 → closes
        assertEquals("window must close after 3 consecutive over-base samples", 20, burst.getLimit());
    }

    public void testBurstWindowDoesNotCloseForBelowBaseSamples() {
        FixedLimit base = new FixedLimit(20);
        BurstAwareLimit burst = new BurstAwareLimit(base, 5, 3, 5);

        // inflight in burst range but below base (< 20) — should not advance close counter.
        burst.onSample(0, RTT, 18, false);
        burst.onSample(0, RTT, 19, false);
        burst.onSample(0, RTT, 15, false);
        assertEquals("samples below base must not close the burst window", 25, burst.getLimit());
    }

    public void testNonSustainingSpikesDoNotCloseWindow() {
        FixedLimit base = new FixedLimit(20);
        BurstAwareLimit burst = new BurstAwareLimit(base, 5, 3, 5);

        // Spike above base (counter=1), then quiet (counter resets), then spike (counter=1 again).
        burst.onSample(0, RTT, 20, false); // counter=1
        burst.onSample(0, RTT, 18, false); // resets to 0
        burst.onSample(0, RTT, 21, false); // counter=1
        burst.onSample(0, RTT, 17, false); // resets to 0
        assertEquals("non-sustaining spikes must not close burst window", 25, burst.getLimit());
    }

    public void testBurstWindowReopensAfterSustainedQuiet() {
        FixedLimit base = new FixedLimit(20);
        BurstAwareLimit burst = new BurstAwareLimit(base, 5, 2, 3);

        // Close the window.
        burst.onSample(0, RTT, 20, false);
        burst.onSample(0, RTT, 21, false);
        assertEquals(20, burst.getLimit());

        burst.onSample(0, RTT, 18, false); // quiet counter=1
        burst.onSample(0, RTT, 19, false); // quiet counter=2
        assertEquals("burst window must remain closed before recovery threshold", 20, burst.getLimit());

        burst.onSample(0, RTT, 17, false); // quiet counter=3 → reopens
        assertEquals("burst window must reopen after sustained quiet", 25, burst.getLimit());
    }

    public void testShortQuietPeriodDoesNotReopen() {
        FixedLimit base = new FixedLimit(20);
        BurstAwareLimit burst = new BurstAwareLimit(base, 5, 2, 5);

        burst.onSample(0, RTT, 20, false);
        burst.onSample(0, RTT, 21, false);
        assertEquals(20, burst.getLimit());

        burst.onSample(0, RTT, 18, false); // counter=1
        burst.onSample(0, RTT, 15, false); // counter=2
        burst.onSample(0, RTT, 10, false); // counter=3
        burst.onSample(0, RTT, 19, false); // counter=4 — still < threshold=5
        assertEquals("burst window must not reopen before recovery threshold", 20, burst.getLimit());
    }

    public void testAboveBaseInflightResetsQuietCounter() {
        FixedLimit base = new FixedLimit(20);
        BurstAwareLimit burst = new BurstAwareLimit(base, 5, 2, 3);

        burst.onSample(0, RTT, 20, false);
        burst.onSample(0, RTT, 21, false);
        assertEquals(20, burst.getLimit());

        burst.onSample(0, RTT, 15, false); // counter=1
        burst.onSample(0, RTT, 15, false); // counter=2
        burst.onSample(0, RTT, 21, false); // inflight >= base(20) → counter resets to 0
        burst.onSample(0, RTT, 15, false); // counter=1 again
        burst.onSample(0, RTT, 15, false); // counter=2 — still < threshold=3
        assertEquals("quiet counter reset by above-base sample must prevent premature reopen", 20, burst.getLimit());

        burst.onSample(0, RTT, 15, false); // counter=3 → reopens
        assertEquals(25, burst.getLimit());
    }

    public void testIndependentCloseAndRecoveryThresholds() {
        FixedLimit base = new FixedLimit(20);
        // Close after 2 consecutive over-base; recover after 4 consecutive below-base.
        BurstAwareLimit burst = new BurstAwareLimit(base, 5, 2, 4);

        // Close should trigger after exactly 2 over-base samples.
        burst.onSample(0, RTT, 20, false); // counter=1
        assertEquals(25, burst.getLimit());
        burst.onSample(0, RTT, 21, false); // counter=2 → closes
        assertEquals(20, burst.getLimit());

        // Recovery should not trigger until 4 below-base samples.
        burst.onSample(0, RTT, 10, false); // counter=1
        burst.onSample(0, RTT, 11, false); // counter=2
        burst.onSample(0, RTT, 12, false); // counter=3 — not yet (threshold=4)
        assertEquals("recovery must not trigger before threshold=4", 20, burst.getLimit());
        burst.onSample(0, RTT, 13, false); // counter=4 → reopens
        assertEquals(25, burst.getLimit());
    }

    public void testListenerNotifiedOnLimitChange() {
        FixedLimit base = new FixedLimit(20);
        BurstAwareLimit burst = new BurstAwareLimit(base, 5, 1, 1);

        int[] lastNotified = { 0 };
        burst.notifyOnChange(newLimit -> lastNotified[0] = newLimit);

        burst.onSample(0, RTT, 20, false); // 1 over-base sample → closes: limit 25 → 20
        assertEquals("listener must receive new limit on burst close", 20, lastNotified[0]);

        burst.onSample(0, RTT, 10, false); // 1 quiet sample → reopens: limit 20 → 25
        assertEquals("listener must receive new limit on burst reopen", 25, lastNotified[0]);
    }

    public void testGetDelegateReturnsDelegate() {
        FixedLimit base = new FixedLimit(20);
        BurstAwareLimit burst = new BurstAwareLimit(base, 5, 3, 3);
        assertSame(base, burst.getDelegate());
    }
}

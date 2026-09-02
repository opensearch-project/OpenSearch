/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.result.ExecutionResult;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The collector's three contracts: one terminal, plan order regardless of completion order, and no
 * short-circuit on a failure.
 */
public class SubPlanResultCollectorTests extends OpenSearchTestCase {

    /** Bound on the concurrent test's start barrier and joins, so a stuck thread fails rather than hangs. */
    private static final int TIMEOUT_SECONDS = 30;

    /**
     * A plan count the countdown could never work for is rejected at construction, from the collector's own
     * side. Two callers keep it from happening today — the single-plan early return and the sequential branch
     * taken at width 1 — but neither of them pins it here, and the tolerated form of this bug is the worst
     * one available: with the countdown starting at or below zero nothing would ever reach the terminal, the
     * request's listener would never be notified, and its REST channel would stay open forever. Failing loudly
     * is also the fail-secure direction, because the caller constructs the collector inside a completion
     * callback whose wrapper turns a throw into the request's failure.
     */
    public void testRejectsAPlanCountThatCouldNeverCompleteTheCountdown() {
        for (int n : new int[] { -1, 0, 1 }) {
            CapturingListener listener = new CapturingListener();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SubPlanResultCollector(n, listener));
            assertEquals("a fan-out collector needs at least 2 plans, got " + n, e.getMessage());
            assertEquals("the listener must not have been touched", 0, listener.terminalCalls);
        }
        // The boundary the other side of it: 2 is the smallest workable count and must construct.
        assertNotNull(new SubPlanResultCollector(2, new CapturingListener()));
    }

    /**
     * Every plan is gated, so the countdown waits for all {@code n} reports — including plan 0's. The
     * discriminating assertion is the middle one: a countdown started at {@code n - 1} would fire the
     * terminal one report early and deliver a list with a null in slot 0.
     */
    public void testTerminalWaitsForEveryPlanIncludingPlanZero() {
        CapturingListener listener = new CapturingListener();
        SubPlanResultCollector collector = new SubPlanResultCollector(3, listener);

        ExecutionResult zero = result();
        ExecutionResult one = result();
        ExecutionResult two = result();
        // Plan 0 arrives like any other gated plan, and deliberately not first.
        collector.planSucceeded(2, two);
        collector.planSucceeded(1, one);
        assertEquals("the terminal must wait for plan 0 as well", 0, listener.terminalCalls);

        collector.planSucceeded(0, zero);

        assertEquals(1, listener.terminalCalls);
        assertNull("no plan failed: " + listener.failure, listener.failure);
        assertEquals(List.of(zero, one, two), listener.results);
    }

    /**
     * The dispatch range and the collector must agree, and this check is the only thing stopping them from
     * disagreeing. A dispatch that skips plan 0 leaves the countdown one report short forever — a REST
     * channel held open — so it is failed loudly instead, and a plan count that is not this collector's own
     * is the same class of bug one argument further out.
     */
    public void testRejectsADispatchRangeTheCountdownCouldNotComplete() {
        CapturingListener skipsPlanZero = new CapturingListener();
        assertFalse(new SubPlanResultCollector(3, skipsPlanZero).expectGatedRange(1, 3));
        assertEquals("the request must be failed, exactly once", 1, skipsPlanZero.terminalCalls);
        assertEquals("a fan-out collector of 3 plans cannot be driven by a dispatch of plans [1, 3)", skipsPlanZero.failure.getMessage());

        CapturingListener wrongPlanCount = new CapturingListener();
        assertFalse(new SubPlanResultCollector(3, wrongPlanCount).expectGatedRange(0, 4));
        assertEquals(1, wrongPlanCount.terminalCalls);

        // The real pairing must pass, or the check would break the dispatch it protects.
        CapturingListener ok = new CapturingListener();
        assertTrue(new SubPlanResultCollector(3, ok).expectGatedRange(0, 3));
        assertEquals("a matching pairing must not touch the listener", 0, ok.terminalCalls);
    }

    public void testFiresOnceWhenAllSlotsFilled() {
        CapturingListener listener = new CapturingListener();
        SubPlanResultCollector collector = new SubPlanResultCollector(3, listener);

        ExecutionResult zero = result();
        ExecutionResult one = result();
        ExecutionResult two = result();
        collector.planSucceeded(0, zero);
        // Reverse completion order on purpose: the emitted order is by plan index, not by arrival.
        collector.planSucceeded(2, two);
        assertEquals("the listener must not fire before the last plan reports", 0, listener.terminalCalls);
        collector.planSucceeded(1, one);

        assertEquals(1, listener.terminalCalls);
        assertNull(listener.failure);
        assertEquals(List.of(zero, one, two), listener.results);
    }

    public void testDoesNotShortCircuitOnFailure() {
        CapturingListener listener = new CapturingListener();
        SubPlanResultCollector collector = new SubPlanResultCollector(3, listener);
        collector.planSucceeded(0, result());

        collector.planFailed(new IllegalStateException("plan 1 failed"));
        assertEquals("short-circuiting would abandon plan 2 while it is still running", 0, listener.terminalCalls);

        collector.planSucceeded(2, result());
        assertEquals(1, listener.terminalCalls);
        assertNotNull(listener.failure);
    }

    /**
     * One failed {@code _search}, <b>one</b> internal exception on the wire — never one per sub-plan.
     */
    public void testSiblingFailuresAreLoggedNotCarriedToTheClient() throws Exception {
        CapturingListener listener = new CapturingListener();
        SubPlanResultCollector collector = new SubPlanResultCollector(3, listener);
        collector.planSucceeded(0, result());

        IllegalStateException first = new IllegalStateException("first");
        IllegalStateException second = new IllegalStateException("second");
        List<Throwable> logged = new ArrayList<>();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(SubPlanResultCollector.class))) {
            appender.addExpectation(new ThrownCapturingExpectation(logged));
            collector.planFailed(first);
            collector.planFailed(second);
        }

        assertSame("the first failure offered is the one the request reports", first, listener.failure);
        assertEquals(
            "a sibling attached with addSuppressed would be rendered to the client too: "
                + Arrays.toString(listener.failure.getSuppressed()),
            0,
            listener.failure.getSuppressed().length
        );
        assertEquals("the sibling must be reported server-side, or the fix loses it: " + logged, List.of(second), logged);
        assertEquals(1, listener.terminalCalls);
    }

    /** A plan that could not be dispatched at all still has to drive the countdown to zero. */
    public void testPreDispatchFailureStillDrivesCountdownToZero() {
        CapturingListener listener = new CapturingListener();
        SubPlanResultCollector collector = new SubPlanResultCollector(2, listener);
        collector.planSucceeded(0, result());

        collector.planFailed(new IllegalStateException("never dispatched"));

        assertEquals("an undispatched plan must not leave the listener uncompleted", 1, listener.terminalCalls);
        assertNotNull(listener.failure);
    }

    /**
     * The {@code AtomicArray.asList()} hazard, stated as a test: on a partial failure nothing may escape
     * through {@code onResponse}, least of all a list shorter than the plan count that looks complete.
     */
    public void testNeverReturnsShortListOnPartialFailure() {
        CapturingListener listener = new CapturingListener();
        SubPlanResultCollector collector = new SubPlanResultCollector(3, listener);
        collector.planSucceeded(0, result());

        collector.planSucceeded(1, result());
        collector.planFailed(new IllegalStateException("plan 2 failed"));

        assertNull("a partial failure must not deliver results", listener.results);
        assertNotNull(listener.failure);
        assertEquals(1, listener.terminalCalls);
    }

    /**
     * The fail-secure guard on the success branch: a slot that was never filled (a wiring bug, not a
     * runtime condition) fails the request rather than handing a caller a list with a null in it, which
     * the caller reads positionally.
     */
    public void testMissingSlotFailsRatherThanDeliveringNulls() {
        CapturingListener listener = new CapturingListener();
        SubPlanResultCollector collector = new SubPlanResultCollector(3, listener);
        // Three reports drive the countdown to its terminal, but one plan reports twice and slot 0 is never
        // filled -- the shape a mis-wired dispatch produces. Plan 0 is deliberately never reported.
        collector.planSucceeded(1, result());
        collector.planSucceeded(2, result());
        collector.planSucceeded(2, result());

        assertNull(listener.results);
        assertTrue(listener.failure instanceof IllegalStateException);
        assertEquals(1, listener.terminalCalls);
    }

    /** n = 9, nine reporters, one terminal. Run with {@code -Dtests.iters=100} for the race. */
    public void testConcurrentReportsFireListenerExactlyOnce() throws Exception {
        CapturingListener listener = new CapturingListener();
        SubPlanResultCollector collector = new SubPlanResultCollector(9, listener);

        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        List<ExecutionResult> expected = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            final int idx = i;
            ExecutionResult result = result();
            expected.add(result);
            Thread thread = new Thread(() -> {
                try {
                    assertTrue(start.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                collector.planSucceeded(idx, result);
            }, "reporter-" + idx);
            threads.add(thread);
            thread.start();
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            assertFalse("reporter thread did not finish", thread.isAlive());
        }

        assertEquals(1, listener.terminalCalls);
        assertNotNull(listener.results);
        assertEquals(9, listener.results.size());
        for (int i = 0; i < 9; i++) {
            assertSame("slot " + i + " must hold its own plan's result", expected.get(i), listener.results.get(i));
        }
    }

    /** Collects the {@code thrown} of every event the collector logs, i.e. the failures it reported itself. */
    private static class ThrownCapturingExpectation implements MockLogAppender.LoggingExpectation {

        private final List<Throwable> thrown;

        ThrownCapturingExpectation(List<Throwable> thrown) {
            this.thrown = thrown;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getThrown() != null) {
                thrown.add(event.getThrown());
            }
        }

        @Override
        public void assertMatched() {
            // The test asserts the contents; an expectation that failed on "no events" would say the same
            // thing twice and with a worse message.
        }
    }

    private static ExecutionResult result() {
        return new ExecutionResult(
            new QueryPlans.QueryPlan(QueryPlans.Type.AGGREGATION, TestUtils.createTestRelNode(), null),
            List.<Object[]>of(new Object[] { COUNTER.incrementAndGet() })
        );
    }

    private static final AtomicInteger COUNTER = new AtomicInteger();

    /** Records the single terminal callback, so a double completion or a silent success is visible. */
    private static class CapturingListener implements ActionListener<List<ExecutionResult>> {

        private List<ExecutionResult> results;
        private Exception failure;
        private int terminalCalls;

        @Override
        public void onResponse(List<ExecutionResult> executionResults) {
            terminalCalls++;
            this.results = executionResults;
        }

        @Override
        public void onFailure(Exception e) {
            terminalCalls++;
            this.failure = e;
        }
    }
}

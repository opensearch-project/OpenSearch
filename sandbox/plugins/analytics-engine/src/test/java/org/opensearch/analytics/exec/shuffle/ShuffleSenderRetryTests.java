/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.shuffle;

import org.opensearch.analytics.exec.action.AnalyticsShuffleDataRequest;
import org.opensearch.analytics.exec.action.AnalyticsShuffleDataResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class ShuffleSenderRetryTests extends OpenSearchTestCase {

    private static AnalyticsShuffleDataRequest req() {
        return new AnalyticsShuffleDataRequest("q1", 0, "left", 0, new byte[] { 1, 2, 3 }, false);
    }

    public void testSuccessfulSendNoRetry() {
        AtomicInteger sendAttempts = new AtomicInteger();
        BiConsumer<AnalyticsShuffleDataRequest, ActionListener<AnalyticsShuffleDataResponse>> sender = (r, listener) -> {
            sendAttempts.incrementAndGet();
            listener.onResponse(new AnalyticsShuffleDataResponse(/* backpressureRejected */ false));
        };
        AtomicReference<AnalyticsShuffleDataResponse> result = new AtomicReference<>();
        ShuffleSenderRetry.sendWithRetry(req(), sender, noopScheduler(), ActionListener.wrap(result::set, e -> fail(e.getMessage())));
        assertNotNull(result.get());
        assertFalse(result.get().isBackpressureRejected());
        assertEquals(1, sendAttempts.get());
    }

    public void testBackpressureRetriesUntilAccepted() {
        AtomicInteger sendAttempts = new AtomicInteger();
        BiConsumer<AnalyticsShuffleDataRequest, ActionListener<AnalyticsShuffleDataResponse>> sender = (r, listener) -> {
            int attempt = sendAttempts.incrementAndGet();
            if (attempt < 3) {
                listener.onResponse(AnalyticsShuffleDataResponse.backpressureReject());
            } else {
                listener.onResponse(new AnalyticsShuffleDataResponse(false));
            }
        };
        AtomicReference<AnalyticsShuffleDataResponse> result = new AtomicReference<>();
        ShuffleSenderRetry.sendWithRetry(req(), sender, inlineScheduler(), ActionListener.wrap(result::set, e -> fail(e.getMessage())));
        assertNotNull(result.get());
        assertFalse("third attempt must succeed", result.get().isBackpressureRejected());
        assertEquals(3, sendAttempts.get());
    }

    public void testGiveUpWhenWaitCeilingReached() {
        AtomicInteger sendAttempts = new AtomicInteger();
        BiConsumer<AnalyticsShuffleDataRequest, ActionListener<AnalyticsShuffleDataResponse>> sender = (r, listener) -> {
            sendAttempts.incrementAndGet();
            listener.onResponse(AnalyticsShuffleDataResponse.backpressureReject());
        };
        AtomicReference<AnalyticsShuffleDataResponse> result = new AtomicReference<>();
        // Wait ceiling of 0 => already out of time at the first reject, so it gives up immediately.
        // Two bounds exist: the attempt budget (8, which trips first in production) and this wall-clock
        // ceiling, which exists because attempts alone do not bound TIME as the backoff grows.
        ShuffleSenderRetry.sendWithRetry(req(), sender, inlineScheduler(), ActionListener.wrap(result::set, e -> fail(e.getMessage())), 0L);
        assertNotNull(result.get());
        assertTrue("final result must still reflect reject", result.get().isBackpressureRejected());
        assertEquals("a zero wait ceiling must give up after a single attempt", 1, sendAttempts.get());
    }

    /**
     * Every scheduled backoff must be non-negative and within the cap. Regression guard: the backoff
     * was computed as {@code initial << (attempt-1)}, and Java's {@code <<} on a long uses only the LOW
     * 6 BITS of the shift count — so past ~32 attempts it overflowed NEGATIVE and the scheduler threw
     * "duration cannot be negative", failing the query. Unreachable at the original 8-attempt budget;
     * reachable as soon as the budget grew to serve steady-state pacing.
     */
    public void testBackoffNeverNegativeAcrossTheFullAttemptBudget() {
        // Accept at attempt 200 rather than spinning on a wall-clock ceiling: the inline scheduler
        // recurses synchronously, so an open-ended spin would overflow the stack rather than test the
        // backoff. 200 comfortably passes the wrap point — the old formula went negative once
        // `attempt-1` reached the 59..63 range (20 << 59 overflows a long).
        final int acceptAt = 200;
        List<Long> delays = new ArrayList<>();
        AtomicInteger sendAttempts = new AtomicInteger();
        BiConsumer<AnalyticsShuffleDataRequest, ActionListener<AnalyticsShuffleDataResponse>> sender = (r, listener) -> {
            if (sendAttempts.incrementAndGet() < acceptAt) {
                listener.onResponse(AnalyticsShuffleDataResponse.backpressureReject());
            } else {
                listener.onResponse(new AnalyticsShuffleDataResponse(false));
            }
        };
        BiConsumer<Long, Runnable> recordingScheduler = (delay, task) -> {
            delays.add(delay);
            task.run();
        };
        AtomicReference<AnalyticsShuffleDataResponse> result = new AtomicReference<>();
        // Explicit 200-attempt budget: the production budget is 8 (shift <= 7), which would never reach
        // the wrap point this guards. A generous wait ceiling keeps TIME from ending the loop first.
        ShuffleSenderRetry.sendWithRetry(
            req(),
            sender,
            recordingScheduler,
            ActionListener.wrap(result::set, e -> fail(e.getMessage())),
            TimeUnit.MINUTES.toMillis(5),
            acceptAt
        );

        assertEquals(acceptAt, sendAttempts.get());
        assertEquals("every reject must have scheduled exactly one retry", acceptAt - 1, delays.size());
        for (int i = 0; i < delays.size(); i++) {
            long d = delays.get(i);
            assertTrue("backoff #" + i + " must be non-negative, was " + d, d >= 0);
            assertTrue("backoff #" + i + " must respect the cap, was " + d, d <= 5_000);
        }
    }

    public void testTransportFailureBubblesUpWithoutRetry() {
        AtomicInteger sendAttempts = new AtomicInteger();
        BiConsumer<AnalyticsShuffleDataRequest, ActionListener<AnalyticsShuffleDataResponse>> sender = (r, listener) -> {
            sendAttempts.incrementAndGet();
            listener.onFailure(new RuntimeException("node unreachable"));
        };
        List<Exception> failures = new ArrayList<>();
        ShuffleSenderRetry.sendWithRetry(
            req(),
            sender,
            inlineScheduler(),
            ActionListener.wrap(r -> fail("should have failed"), failures::add)
        );
        assertEquals(1, sendAttempts.get());
        assertEquals(1, failures.size());
        assertTrue(failures.get(0).getMessage().contains("node unreachable"));
    }

    /** Runs the retry inline, no sleep — tests focus on retry logic, not timing. */
    private BiConsumer<Long, Runnable> inlineScheduler() {
        return (delay, r) -> r.run();
    }

    /** Fails loudly if anything tries to schedule a retry — used when no retry is expected. */
    private BiConsumer<Long, Runnable> noopScheduler() {
        return (delay, r) -> fail("scheduler was invoked but should not have been: delay=" + delay);
    }
}

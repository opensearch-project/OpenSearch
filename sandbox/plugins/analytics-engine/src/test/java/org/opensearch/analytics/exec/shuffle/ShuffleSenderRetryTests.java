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

    public void testGiveUpAfterMaxAttempts() {
        AtomicInteger sendAttempts = new AtomicInteger();
        BiConsumer<AnalyticsShuffleDataRequest, ActionListener<AnalyticsShuffleDataResponse>> sender = (r, listener) -> {
            sendAttempts.incrementAndGet();
            listener.onResponse(AnalyticsShuffleDataResponse.backpressureReject());
        };
        AtomicReference<AnalyticsShuffleDataResponse> result = new AtomicReference<>();
        ShuffleSenderRetry.sendWithRetry(req(), sender, inlineScheduler(), ActionListener.wrap(result::set, e -> fail(e.getMessage())));
        assertNotNull(result.get());
        assertTrue("final result must still reflect reject", result.get().isBackpressureRejected());
        // Default max attempts is 8.
        assertEquals(8, sendAttempts.get());
    }

    public void testTransportFailureBubblesUpWithoutRetry() {
        AtomicInteger sendAttempts = new AtomicInteger();
        BiConsumer<AnalyticsShuffleDataRequest, ActionListener<AnalyticsShuffleDataResponse>> sender = (r, listener) -> {
            sendAttempts.incrementAndGet();
            listener.onFailure(new RuntimeException("node unreachable"));
        };
        List<Exception> failures = new ArrayList<>();
        ShuffleSenderRetry.sendWithRetry(req(), sender, inlineScheduler(), ActionListener.wrap(r -> fail("should have failed"), failures::add));
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

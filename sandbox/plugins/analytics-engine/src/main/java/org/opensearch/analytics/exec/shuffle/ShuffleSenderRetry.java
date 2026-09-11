/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.shuffle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.analytics.exec.action.AnalyticsShuffleDataRequest;
import org.opensearch.analytics.exec.action.AnalyticsShuffleDataResponse;
import org.opensearch.core.action.ActionListener;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Sender-side retry loop for {@link AnalyticsShuffleDataRequest}. When the worker responds with
 * {@code backpressureRejected=true}, reschedule the same request with exponential backoff up to
 * {@code maxAttempts}. Retrying is non-optional — the worker's transport handler must not block
 * (OpenSearch transport threads), so a full buffer translates to a reject + sender retry.
 *
 * <p>This helper does not cover retrying on transport errors (node unreachable, shard not found);
 * those are out of scope for M2 and tracked as follow-up per doc 90 §3 / doc 65 out-of-scope #5.
 * It also assumes the underlying transport client provides a way to schedule a delayed retry
 * — the {@code scheduler} {@link BiConsumer} abstraction lets callers wire whatever ThreadPool /
 * Scheduler they have (test: same-thread executor; production: {@code ThreadPool.schedule}).
 *
 * @opensearch.internal
 */
public final class ShuffleSenderRetry {

    private static final Logger LOGGER = LogManager.getLogger(ShuffleSenderRetry.class);

    /**
     * Backoff for a TRANSIENT backpressure reject: 8 attempts, 50ms doubling to a 5s cap (~15s total).
     *
     * <p>These are the original values, deliberately chosen for the case a reject actually represents
     * today — another query momentarily filled the node budget. Failing a contended query reasonably
     * fast is safer than queueing behind an accumulation that could OOM the node.
     *
     * <p>They were briefly raised (500 attempts, then unbounded with a 30-minute ceiling) to serve the
     * in-flight window's pacing, where a full window is a NORMAL steady state rather than a blip. That
     * pacing scheme is disabled ({@code analytics.mpp.shuffle.stream_window} defaults to 0) because it
     * deadlocks — see that setting. Raising these again is only correct alongside a real producer-side
     * pause; a re-send storm on the bounded GENERIC pool is what hung all seven probe queries at sf=10.
     */
    private static final int DEFAULT_MAX_ATTEMPTS = 8;
    private static final long DEFAULT_INITIAL_BACKOFF_MILLIS = 50;
    private static final long DEFAULT_MAX_BACKOFF_MILLIS = 5_000;

    /**
     * Wall-clock ceiling on backpressure waiting, independent of the attempt count.
     *
     * <p>Kept as a second bound because attempts alone do not bound TIME (the backoff grows), and a
     * caller must not wait unboundedly on a consumer that will never drain. 2 minutes is well past the
     * ~15s the attempt budget allows, so in practice the attempt count is what trips first; this is the
     * backstop for a pathologically slow scheduler.
     */
    private static final long DEFAULT_MAX_TOTAL_WAIT_MILLIS = TimeUnit.MINUTES.toMillis(2);

    private ShuffleSenderRetry() {}

    /**
     * Dispatch {@code request} via {@code sender}; on backpressure reject, re-dispatch after
     * exponential backoff (50ms, 100ms, 200ms, ..., capped at 5s). Caps total attempts at
     * {@code maxAttempts}.
     *
     * @param request        the shuffle payload to deliver
     * @param sender         performs one send attempt (e.g. transport client wrapper)
     * @param scheduler      schedules a Runnable after a given millis delay — one call per retry
     * @param finalListener  notified once the shuffle write either succeeds (non-rejected response)
     *                       or exhausts retries / fails for a non-backpressure reason
     */
    public static void sendWithRetry(
        AnalyticsShuffleDataRequest request,
        BiConsumer<AnalyticsShuffleDataRequest, ActionListener<AnalyticsShuffleDataResponse>> sender,
        BiConsumer<Long, Runnable> scheduler,
        ActionListener<AnalyticsShuffleDataResponse> finalListener
    ) {
        sendWithRetry(
            request,
            sender,
            scheduler,
            finalListener,
            DEFAULT_MAX_ATTEMPTS,
            DEFAULT_INITIAL_BACKOFF_MILLIS,
            1,
            System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DEFAULT_MAX_TOTAL_WAIT_MILLIS)
        );
    }

    /** {@link #sendWithRetry} with an explicit wait ceiling. Visible for tests, which must not inherit
     *  the production ceiling when driving the loop with an inline scheduler. */
    static void sendWithRetry(
        AnalyticsShuffleDataRequest request,
        BiConsumer<AnalyticsShuffleDataRequest, ActionListener<AnalyticsShuffleDataResponse>> sender,
        BiConsumer<Long, Runnable> scheduler,
        ActionListener<AnalyticsShuffleDataResponse> finalListener,
        long maxTotalWaitMillis
    ) {
        sendWithRetry(request, sender, scheduler, finalListener, maxTotalWaitMillis, DEFAULT_MAX_ATTEMPTS);
    }

    /** {@link #sendWithRetry} with an explicit wait ceiling AND attempt budget. Visible for tests so the
     *  backoff arithmetic can be driven past the shift width that used to overflow, independently of the
     *  production budget. */
    static void sendWithRetry(
        AnalyticsShuffleDataRequest request,
        BiConsumer<AnalyticsShuffleDataRequest, ActionListener<AnalyticsShuffleDataResponse>> sender,
        BiConsumer<Long, Runnable> scheduler,
        ActionListener<AnalyticsShuffleDataResponse> finalListener,
        long maxTotalWaitMillis,
        int maxAttempts
    ) {
        sendWithRetry(
            request,
            sender,
            scheduler,
            finalListener,
            maxAttempts,
            DEFAULT_INITIAL_BACKOFF_MILLIS,
            1,
            System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxTotalWaitMillis)
        );
    }

    private static void sendWithRetry(
        AnalyticsShuffleDataRequest request,
        BiConsumer<AnalyticsShuffleDataRequest, ActionListener<AnalyticsShuffleDataResponse>> sender,
        BiConsumer<Long, Runnable> scheduler,
        ActionListener<AnalyticsShuffleDataResponse> finalListener,
        int maxAttempts,
        long initialBackoffMillis,
        int attempt,
        long deadlineNanos
    ) {
        sender.accept(request, new ActionListener<>() {
            @Override
            public void onResponse(AnalyticsShuffleDataResponse response) {
                if (!response.isBackpressureRejected()) {
                    finalListener.onResponse(response);
                    return;
                }
                // Give up on TIME, not on attempt count (see DEFAULT_MAX_TOTAL_WAIT_MILLIS): a paced
                // producer legitimately waits out the consumer's whole build phase.
                boolean outOfTime = deadlineNanos - System.nanoTime() <= 0;
                if (outOfTime || attempt >= maxAttempts) {
                    LOGGER.warn(
                        new ParameterizedMessage(
                            "Shuffle sender gave up after {} attempts ({}) for query={}, stage={}, partition={}",
                            attempt,
                            outOfTime ? "wait ceiling reached" : "attempt ceiling reached",
                            request.getQueryId(),
                            request.getTargetStageId(),
                            request.getPartitionIndex()
                        )
                    );
                    finalListener.onResponse(response);
                    return;
                }
                // Clamp the shift exponent. Java's << on a long uses only the LOW 6 BITS of the shift
                // count, so `initial << (attempt-1)` silently wraps once attempt-1 >= 64 and overflows
                // NEGATIVE well before that — producing `duration cannot be negative` from the
                // scheduler and failing the query. This was unreachable at the old 8-attempt budget
                // (shift <= 7) and became reachable the moment the budget grew for pacing.
                int shift = Math.min(attempt - 1, 32);
                long backoff = Math.min(DEFAULT_MAX_BACKOFF_MILLIS, initialBackoffMillis << shift);
                LOGGER.debug(
                    "Shuffle backpressure-rejected, retrying in {}ms (attempt {}/{}): query={}, stage={}, partition={}",
                    backoff,
                    attempt + 1,
                    maxAttempts,
                    request.getQueryId(),
                    request.getTargetStageId(),
                    request.getPartitionIndex()
                );
                scheduler.accept(
                    backoff,
                    () -> sendWithRetry(
                        request,
                        sender,
                        scheduler,
                        finalListener,
                        maxAttempts,
                        initialBackoffMillis,
                        attempt + 1,
                        deadlineNanos
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                // Non-backpressure transport error: M2 scope is backpressure retry only; bubble up.
                finalListener.onFailure(e);
            }
        });
    }
}

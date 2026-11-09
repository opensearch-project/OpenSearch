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
import org.opensearch.analytics.exec.action.AnalyticsShuffleDataRequest;
import org.opensearch.analytics.exec.action.AnalyticsShuffleDataResponse;
import org.opensearch.core.action.ActionListener;

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

    private static final int DEFAULT_MAX_ATTEMPTS = 8;
    private static final long DEFAULT_INITIAL_BACKOFF_MILLIS = 50;
    private static final long DEFAULT_MAX_BACKOFF_MILLIS = 5_000;

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
        sendWithRetry(request, sender, scheduler, finalListener, DEFAULT_MAX_ATTEMPTS, DEFAULT_INITIAL_BACKOFF_MILLIS, 1);
    }

    private static void sendWithRetry(
        AnalyticsShuffleDataRequest request,
        BiConsumer<AnalyticsShuffleDataRequest, ActionListener<AnalyticsShuffleDataResponse>> sender,
        BiConsumer<Long, Runnable> scheduler,
        ActionListener<AnalyticsShuffleDataResponse> finalListener,
        int maxAttempts,
        long initialBackoffMillis,
        int attempt
    ) {
        sender.accept(request, new ActionListener<>() {
            @Override
            public void onResponse(AnalyticsShuffleDataResponse response) {
                if (!response.isBackpressureRejected()) {
                    finalListener.onResponse(response);
                    return;
                }
                if (attempt >= maxAttempts) {
                    LOGGER.warn(
                        "Shuffle sender gave up after {} attempts for query={}, stage={}, partition={}",
                        attempt,
                        request.getQueryId(),
                        request.getTargetStageId(),
                        request.getPartitionIndex()
                    );
                    finalListener.onResponse(response);
                    return;
                }
                long backoff = Math.min(DEFAULT_MAX_BACKOFF_MILLIS, initialBackoffMillis << (attempt - 1));
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
                    () -> sendWithRetry(request, sender, scheduler, finalListener, maxAttempts, initialBackoffMillis, attempt + 1)
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

/**
 * Common utility for recording wall-clock elapsed time around stat-emitting callables.
 * Eliminates the repeated boilerplate of:
 *
 * <pre>{@code
 *     long start = System.nanoTime();
 *     try { work(); }
 *     finally { tracker.addXxxTimeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)); }
 * }</pre>
 *
 * <p>Usage examples:
 * <pre>{@code
 *     // Time-only (no success/failure counters):
 *     recordTimeMillis(() -> indexWriter.forceMerge(1, true), stats::addFlushForceMergeTimeMillis);
 *
 *     // Time + outcome (success counter on clean return, failure counter on throw):
 *     recordOutcome(
 *         () -> { doWork(); return result; },
 *         tracker::addCommitTimeMillis,
 *         tracker::incCommitTotal,
 *         tracker::incCommitFailures
 *     );
 * }</pre>
 *
 * <p>All variants record elapsed time even when the wrapped work throws.
 *
 * @opensearch.experimental
 */
public final class StatsRecorder {

    private StatsRecorder() {}

    /** Closure that may throw {@link IOException}. */
    @FunctionalInterface
    public interface IORunnable {
        void run() throws IOException;
    }

    /** Closure with a return value that may throw {@link IOException}. */
    @FunctionalInterface
    public interface IOSupplier<T> {
        T get() throws IOException;
    }

    /**
     * Runs {@code work} and reports elapsed wall-clock millis to {@code timeRecorder}.
     * Time is reported even if {@code work} throws.
     */
    public static void recordTimeMillis(IORunnable work, LongConsumer timeRecorder) throws IOException {
        Objects.requireNonNull(work, "work");
        Objects.requireNonNull(timeRecorder, "timeRecorder");
        long start = System.nanoTime();
        try {
            work.run();
        } finally {
            timeRecorder.accept(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        }
    }

    /**
     * Runs {@code work} and reports time + outcome:
     * <ul>
     *   <li>{@code timeRecorder} always fires (in {@code finally})</li>
     *   <li>{@code onSuccess} fires only on a clean return</li>
     *   <li>{@code onFailure} fires only when {@code work} throws</li>
     * </ul>
     */
    public static <T> T recordOutcome(IOSupplier<T> work, LongConsumer timeRecorder, Runnable onSuccess, Runnable onFailure)
        throws IOException {
        Objects.requireNonNull(work, "work");
        Objects.requireNonNull(timeRecorder, "timeRecorder");
        Objects.requireNonNull(onSuccess, "onSuccess");
        Objects.requireNonNull(onFailure, "onFailure");
        long start = System.nanoTime();
        boolean threw = false;
        try {
            return work.get();
        } catch (Throwable t) {
            threw = true;
            onFailure.run();
            throw t;
        } finally {
            timeRecorder.accept(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            if (threw == false) {
                onSuccess.run();
            }
        }
    }

    /**
     * Void-returning variant of {@link #recordOutcome(IOSupplier, LongConsumer, Runnable, Runnable)}.
     */
    public static void recordOutcome(IORunnable work, LongConsumer timeRecorder, Runnable onSuccess, Runnable onFailure)
        throws IOException {
        Objects.requireNonNull(work, "work");
        recordOutcome(() -> {
            work.run();
            return null;
        }, timeRecorder, onSuccess, onFailure);
    }
}

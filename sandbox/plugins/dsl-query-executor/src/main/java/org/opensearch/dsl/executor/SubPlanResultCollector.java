/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.core.action.ActionListener;
import org.opensearch.dsl.result.ExecutionResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Gathers the results of one query's concurrently executing sub-plans into <b>plan order</b> and fires
 * the request's listener exactly once, when the last of them has reported.
 */
final class SubPlanResultCollector {

    private static final Logger logger = LogManager.getLogger(SubPlanResultCollector.class);

    private final int n;
    private final AtomicArray<ExecutionResult> slots;
    private final AtomicInteger pending;
    private final ConcurrentLinkedQueue<Exception> failures = new ConcurrentLinkedQueue<>();
    private final ActionListener<List<ExecutionResult>> outer;

    /**
     * Every plan of the query is gated, so every plan reports through {@link #planSucceeded} /
     * {@link #planFailed} and the countdown starts at {@code n}.
     *
     * @param n total number of plans in the query; must be at least 2
     * @param outer the request's listener, fired exactly once
     * @throws IllegalArgumentException if {@code n < 2}, which would make the countdown unable to complete
     */
    SubPlanResultCollector(int n, ActionListener<List<ExecutionResult>> outer) {
        if (n < 2) {
            // Rejected rather than tolerated, because the tolerated form is a HANG: with n <= 1 the
            // countdown starts at or below 0, so finish() never runs, the request's listener is never fired
            // and the REST channel is held open until it times out.
            throw new IllegalArgumentException("a fan-out collector needs at least 2 plans, got " + n);
        }
        this.n = n;
        this.slots = new AtomicArray<>(n);
        this.pending = new AtomicInteger(n);
        this.outer = outer;
    }

    /**
     * Rejects a dispatch that could not drive this countdown to its terminal — checked on the caller's
     * thread, before the first plan goes out, so nothing is in flight and no permit is held.
     *
     * @param from the first plan index the caller is about to dispatch
     * @param n the caller's plan count, i.e. the exclusive upper bound of its dispatch
     * @return {@code false} when the pairing cannot terminate, having already failed the request's listener
     */
    boolean expectGatedRange(int from, int n) {
        if (n == this.n && from == 0) {
            return true;
        }
        IllegalStateException e = new IllegalStateException(
            "a fan-out collector of " + this.n + " plans cannot be driven by a dispatch of plans [" + from + ", " + n + ")"
        );
        logger.error("dsl.fanout dispatch/report-count mismatch; the query is failed rather than hung", e);
        outer.onFailure(e);
        return false;
    }

    /**
     * Records a fanned-out plan's result and counts down.
     *
     * @param index the plan's index in {@code QueryPlans.getAll()}
     * @param result that plan's result
     */
    void planSucceeded(int index, ExecutionResult result) {
        slots.set(index, result);
        countDown();
    }

    /**
     * Records a fanned-out plan's failure and counts down. Called for a plan that failed asynchronously
     * <i>and</i> for one that could not be dispatched at all — a plan that never decremented would leave
     * the listener uncompleted, i.e. a hung REST channel.
     *
     * @param e the failure
     */
    void planFailed(Exception e) {
        failures.offer(e);
        countDown();
    }

    /**
     * The single terminal. Both the success and the failure path go through here, rather than each
     * testing the countdown itself, so "fires exactly once" is a property of one line of code.
     */
    private void countDown() {
        if (pending.decrementAndGet() == 0) {
            finish();
        }
    }

    private void finish() {
        // Drained once, then never read again: poll() hands over the primary and the loop reports the rest,
        // so no failure is lost and none is reported twice.
        Exception primary = failures.poll();
        if (primary != null) {
            // The siblings are LOGGED, deliberately not attached to the reported exception with
            // addSuppressed(). OpenSearch's REST error rendering walks the suppressed chain, so attaching
            // them would emit one internal exception type and message per failed sub-plan to the client:
            int siblings = 0;
            for (Exception other = failures.poll(); other != null; other = failures.poll()) {
                final int sibling = ++siblings;
                logger.warn(
                    () -> new ParameterizedMessage(
                        "a further sub-plan of this {}-plan query failed (additional failure {}); it is "
                            + "reported here only, never attached to the failure returned to the client, "
                            + "which is [{}]",
                        n,
                        sibling,
                        primary
                    ),
                    other
                );
            }
            outer.onFailure(primary);
            return;
        }
        // Read by index into a fresh list rather than AtomicArray.asList(): that method SKIPS null slots
        // and memoizes the result, so an incompletely filled array would hand back a SHORT list that looks
        // complete — and the wrong value would then be cached forever.
        List<ExecutionResult> results = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            ExecutionResult result = slots.get(i);
            if (result == null) {
                // Unreachable while every plan reports exactly once; kept because the alternative is
                // delivering a list with a null in it to a caller that indexes into it positionally.
                outer.onFailure(new IllegalStateException("sub-plan " + i + " of " + n + " reported neither a result nor a failure"));
                return;
            }
            results.add(result);
        }
        outer.onResponse(results);
    }
}

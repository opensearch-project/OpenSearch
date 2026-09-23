/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import java.util.ArrayDeque;
import java.util.function.BooleanSupplier;

/**
 * Permit-based concurrency queue. Same pattern as
 * {@code AbstractSearchAsyncAction.PendingExecutions} in OpenSearch core.
 *
 * <p>Callers submit work via {@link #tryRun}. If a permit is available the work runs immediately on
 * the caller's thread; otherwise it is queued and drained when a prior execution calls
 * {@link #finishAndRunNext()}.
 *
 * @opensearch.internal
 */
public final class PendingExecutions {
    private final int permits;
    private int permitsTaken = 0;
    private final ArrayDeque<BooleanSupplier> queue = new ArrayDeque<>();

    public PendingExecutions(int permits) {
        assert permits > 0 : "permits must be > 0: " + permits;
        this.permits = permits;
    }

    /**
     * Runs {@code work} if a permit is free, otherwise queues it. Once admitted, {@code work} may
     * decline — it can turn out to have nothing to do by the time its turn comes.
     *
     * <p>{@code true} means it started and owes a {@link #finishAndRunNext()}. {@code false} means it
     * started nothing, so its permit goes to the next queued item — declining without handing the
     * permit on would shrink the window and eventually stall it. Declines are drained in a loop here
     * rather than recursively, so declining a whole fan-out costs constant stack.
     */
    public void tryRun(BooleanSupplier work) {
        BooleanSupplier toExecute = tryQueue(work);
        while (toExecute != null && toExecute.getAsBoolean() == false) {
            toExecute = passPermitOn();
        }
    }

    public void finishAndRunNext() {
        synchronized (this) {
            permitsTaken--;
            assert permitsTaken >= 0 : "illegal permits: " + permitsTaken;
        }
        tryRun(null);
    }

    private synchronized BooleanSupplier tryQueue(BooleanSupplier runnable) {
        BooleanSupplier toExecute = null;
        if (permitsTaken < permits) {
            permitsTaken++;
            toExecute = runnable;
            if (toExecute == null) {
                toExecute = queue.poll();
            }
            if (toExecute == null) {
                permitsTaken--;
            }
        } else if (runnable != null) {
            queue.add(runnable);
        }
        return toExecute;
    }

    /**
     * Hands a decliner's permit to the next queued item, or releases it when nothing is waiting. The
     * count is untouched on a hand-off, so the permit never looks free and the window can't overshoot.
     */
    private synchronized BooleanSupplier passPermitOn() {
        BooleanSupplier next = queue.poll();
        if (next == null) {
            permitsTaken--;
            assert permitsTaken >= 0 : "illegal permits: " + permitsTaken;
        }
        return next;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import java.util.ArrayDeque;

/**
 * Permit-based concurrency queue. Same pattern as
 * {@code AbstractSearchAsyncAction.PendingExecutions} in OpenSearch core.
 *
 * <p>Callers submit work via {@link #tryRun(Runnable)}. If a permit is
 * available the runnable executes immediately on the caller's thread;
 * otherwise it is queued and drained when a prior execution calls
 * {@link #finishAndRunNext()}.
 *
 * @opensearch.internal
 */
public final class PendingExecutions {
    private final int permits;
    private int permitsTaken = 0;
    private final ArrayDeque<Runnable> queue = new ArrayDeque<>();

    public PendingExecutions(int permits) {
        assert permits > 0 : "permits must be > 0: " + permits;
        this.permits = permits;
    }

    public void tryRun(Runnable runnable) {
        Runnable toExecute = tryQueue(runnable);
        if (toExecute != null) {
            toExecute.run();
        }
    }

    public void finishAndRunNext() {
        synchronized (this) {
            permitsTaken--;
            assert permitsTaken >= 0 : "illegal permits: " + permitsTaken;
        }
        tryRun(null);
    }

    private synchronized Runnable tryQueue(Runnable runnable) {
        Runnable toExecute = null;
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
}

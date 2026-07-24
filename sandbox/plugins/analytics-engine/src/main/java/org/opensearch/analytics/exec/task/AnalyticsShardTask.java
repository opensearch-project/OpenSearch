/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.task;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.core.tasks.TaskId;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Data-node shard task representing a single shard fragment execution.
 * <p>Extends {@link SearchShardTask} so that search backpressure and
 * {@code TaskResourceTrackingService} observe and track analytics shard
 * tasks alongside regular search shard tasks. The native-memory tracker
 * in {@code SearchBackpressureService} filters by {@code SearchShardTask},
 * so inheriting from it is the integration point that exposes per-query
 * DataFusion memory to cancellation.
 *
 * <p>Cancelling this task does not cascade to children (inherited behaviour
 * from {@link SearchShardTask}).
 *
 * @opensearch.internal
 */
public class AnalyticsShardTask extends SearchShardTask {

    private final AtomicReference<Runnable> cancellationListener = new AtomicReference<>();
    // Additive cancellation listeners (run in addition to the single-slot one above). Multiple
    // independent concerns register a cancel hook on the SAME task — e.g. the DataFusion engine's
    // native-cancel and the shuffle-buffer cleanup — and the single-slot setter would let one
    // overwrite the other (a leak / missed-cancel). These all fire on cancellation.
    private final Queue<Runnable> cancellationListeners = new ConcurrentLinkedQueue<>();

    public AnalyticsShardTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, description, parentTaskId, headers);
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return false;
    }

    @Override
    public boolean supportsResourceTracking() {
        return true;
    }

    /**
     * Registers a listener that is called exactly once when this task is cancelled.
     * If the task is already cancelled at registration time, the listener fires immediately.
     */
    public void setCancellationListener(Runnable listener) {
        cancellationListener.set(listener);
        if (isCancelled()) {
            Runnable taken = cancellationListener.getAndSet(null);
            if (taken != null) {
                taken.run();
            }
        }
    }

    /** Removes the cancellation listener, e.g. after the query completes normally. */
    public void clearCancellationListener() {
        cancellationListener.set(null);
    }

    /**
     * Registers an ADDITIVE cancellation listener that runs (exactly once) when this task is
     * cancelled, ALONGSIDE any other registered listeners and the single-slot one — it does not
     * replace them. If the task is already cancelled at registration time, the listener fires
     * immediately. Use this (not {@link #setCancellationListener}) when multiple independent
     * concerns each need a cancel hook on the same task.
     */
    public void addCancellationListener(Runnable listener) {
        if (listener == null) {
            return;
        }
        cancellationListeners.add(listener);
        if (isCancelled() && cancellationListeners.remove(listener)) {
            listener.run();
        }
    }

    @Override
    protected void onCancelled() {
        super.onCancelled();
        Runnable listener = cancellationListener.getAndSet(null);
        if (listener != null) {
            listener.run();
        }
        // Drain and run the additive listeners. poll() ensures each runs at most once even if
        // onCancelled races with a late addCancellationListener (which fires inline on its own).
        Runnable additive;
        while ((additive = cancellationListeners.poll()) != null) {
            additive.run();
        }
    }
}

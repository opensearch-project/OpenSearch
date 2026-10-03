/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.search.SearchTask;
import org.opensearch.analytics.spi.NativeTaskIdManager;
import org.opensearch.common.Nullable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.TaskId;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Coordinator-level cancellable task representing a running analytics query.
 * Analogous to {@link SearchTask}.
 * Cancelling this task cascades cancellation to all child shard tasks.
 *
 * @opensearch.internal
 */
public class AnalyticsQueryTask extends SearchTask {

    private static final Logger logger = LogManager.getLogger(AnalyticsQueryTask.class);

    private final String queryId;
    private final TimeValue cancelAfterTimeInterval;
    private final AtomicReference<Runnable> onCancelCallback = new AtomicReference<>();
    /**
     * Additive cancel listeners, kept separate from the single-slot {@link #onCancelCallback} the query
     * driver owns. See {@link #addCancellationListener}.
     */
    private final Queue<Runnable> cancellationListeners = new ConcurrentLinkedQueue<>();
    /**
     * JVM-unique id keying this query's native tracking contexts. {@link #getId()} must
     * not be used for that: task ids are per-node counters and collide in the
     * process-wide native registry when nodes share a JVM (internal test clusters).
     */
    private final long nativeTaskId = NativeTaskIdManager.next();

    public AnalyticsQueryTask(
        long id,
        String type,
        String action,
        String queryId,
        TaskId parentTaskId,
        Map<String, String> headers,
        @Nullable TimeValue cancelAfterTimeInterval
    ) {
        // Pass cancelAfterTimeInterval through unchanged (null when unset) so getCancellationTimeout()
        // returns null and the cluster search.cancel_after_time_interval applies — matching core
        // SearchTask. Coercing null→MINUS_ONE silently disabled the cluster timeout.
        super(id, type, action, (Supplier<String>) () -> "queryId[" + queryId + "]", parentTaskId, headers, cancelAfterTimeInterval);
        this.queryId = queryId;
        this.cancelAfterTimeInterval = cancelAfterTimeInterval;
    }

    public AnalyticsQueryTask(long id, String type, String action, String queryId, TaskId parentTaskId, Map<String, String> headers) {
        this(id, type, action, queryId, parentTaskId, headers, null);
    }

    /** JVM-unique id for this query's native tracking contexts (see field javadoc). */
    public long getNativeTaskId() {
        return nativeTaskId;
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }

    public String getQueryId() {
        return queryId;
    }

    @Nullable
    public TimeValue getCancelAfterTimeInterval() {
        return cancelAfterTimeInterval;
    }

    /**
     * Install a callback to be run when this task is cancelled. Typically called right
     * after task registration by the query driver. The callback runs on whatever thread
     * invokes cancel (transport thread, timeout scheduler, parent cascade); it must be
     * non-blocking and safe from any thread.
     *
     * <p>Replaces any previously installed callback — multi-phase drivers (e.g. M1 broadcast
     * dispatch) install a temporary callback targeting the phase 1 execution, then replace
     * it when phase 2 begins so cancel routes to the active phase's walker.
     *
     * <p>Late-install replay: if this task has already been cancelled by the time
     * {@code setOnCancelCallback} is called, run the new callback immediately on the
     * caller's thread. {@link #onCancelled()} is one-shot, so without replay a callback
     * installed after cancellation would never fire — losing cancel semantics across the
     * broadcast-capture → residual-dispatch handoff in {@code UnifiedDispatch}.
     */
    public void setOnCancelCallback(Runnable callback) {
        onCancelCallback.set(callback);
        // Cancel may have arrived before this install — fire inline if so. onCancelled() is one-shot
        // (it clears the slot via getAndSet(null)), so a callback installed after cancellation would
        // otherwise never fire — losing cancel semantics across the broadcast-capture → residual-dispatch
        // handoff in UnifiedDispatch. Mirrors AnalyticsShardTask.setCancellationListener.
        if (callback != null && isCancelled()) {
            runCallbackOnce();
        }
    }

    /**
     * Registers an ADDITIVE cancellation listener that runs (exactly once) when this task is cancelled,
     * ALONGSIDE {@link #setOnCancelCallback} and any other additive listeners — it does not replace
     * them. If the task is already cancelled at registration time, the listener fires immediately on
     * the caller's thread.
     *
     * <p>Use this, never {@code setOnCancelCallback}, for a concern that is independent of the query
     * driver. The single slot belongs to the driver and is deliberately replaced as dispatch moves
     * between phases, so registering there would silently drop whichever concern installed first.
     *
     * <p>The motivating concern is cancelling in-flight analytics streams. A drain parks in
     * {@code FlightStream.next()} with no deadline, and the drain loop's own {@code stream.cancel(...)}
     * sits in a {@code finally} that the blocked thread never reaches; stage cancellation only flips
     * state flags. So unless cancellation is delivered to the stream from another thread, the read
     * never returns and the query leaks a live task plus a parked thread until the node restarts.
     *
     * <p>Listeners must be non-blocking and safe to run from any thread — cancel is delivered on
     * whichever thread invokes it (transport thread, timeout scheduler, or parent cascade).
     */
    public void addCancellationListener(Runnable listener) {
        if (listener == null) {
            return;
        }
        cancellationListeners.add(listener);
        // Cancel may already have happened; remove-then-run so this listener cannot also be run by a
        // concurrent onCancelled() drain.
        if (isCancelled() && cancellationListeners.remove(listener)) {
            runQuietly(listener);
        }
    }

    /** Deregisters a listener, e.g. once its stream has finished normally and no longer needs cancelling. */
    public void removeCancellationListener(Runnable listener) {
        if (listener != null) {
            cancellationListeners.remove(listener);
        }
    }

    @Override
    protected void onCancelled() {
        runCallbackOnce();
        // Drain rather than iterate: poll() guarantees each listener runs at most once even if
        // onCancelled races a late addCancellationListener (which fires inline on its own).
        Runnable additive;
        while ((additive = cancellationListeners.poll()) != null) {
            runQuietly(additive);
        }
    }

    private void runCallbackOnce() {
        Runnable cb = onCancelCallback.getAndSet(null);
        if (cb != null) {
            runQuietly(cb);
        }
    }

    /**
     * One listener throwing must not strand the others — a half-delivered cancel is what leaves streams
     * parked forever.
     */
    private void runQuietly(Runnable r) {
        try {
            r.run();
        } catch (Exception e) {
            logger.warn(new ParameterizedMessage("[AnalyticsQueryTask] onCancelled callback failed for queryId={}", queryId), e);
        }
    }
}

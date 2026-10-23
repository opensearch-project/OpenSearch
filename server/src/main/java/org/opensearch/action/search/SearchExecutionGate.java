/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.opensearch.action.ActionRequest;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.tasks.Task;

import java.util.function.Consumer;

/**
 * Decides when the coordinator may start a search or a scroll page: at once, after waiting, or never. The search actions
 * run each request through {@link #execute}, which gets a permit from {@link #admit}, runs the request under it, and
 * closes it when the request completes. One per node, bound in {@code Node}; workload management provides it.
 *
 * @opensearch.internal
 */
@InternalApi
@FunctionalInterface
public interface SearchExecutionGate {

    /** The permit for a request admitted without being charged against any limit. */
    Releasable NOOP_PERMIT = () -> {};

    /** A gate that is never active, so {@link #execute} runs every request directly. */
    SearchExecutionGate NOOP = new SearchExecutionGate() {
        @Override
        public boolean isActive() {
            return false;
        }

        @Override
        public void admit(Task task, ActionRequest request, ActionListener<Releasable> listener) {
            listener.onResponse(NOOP_PERMIT);
        }
    };

    /**
     * Whether requests currently need admission. While {@code false}, {@link #execute} runs each request directly and never
     * calls {@link #admit}. Read on every search, so it must be cheap; it may change at any time.
     */
    default boolean isActive() {
        return true;
    }

    /**
     * Decides whether {@code request} may start, completing {@code listener} exactly once with either:
     * <ul>
     *   <li>a non-null permit, which the caller closes once the request completes ({@link #NOOP_PERMIT} when nothing was
     *       charged). Closing runs before the request's listener is notified, so it must be cheap;</li>
     *   <li>the exception the request fails with, without having run. It reaches the client unchanged, so a throttle
     *       rejection is an {@link OpenSearchRejectedExecutionException} (a 429).</li>
     * </ul>
     * Must not block. It may complete inline or later on any thread, with the caller's thread context restored; the request
     * starts on the completing thread, so an asynchronous grant should be delivered on an executor, not a transport thread.
     * The caller never cancels an admission: one that can wait indefinitely must fail once {@code task} is cancelled, and
     * must close any permit it can no longer deliver. {@code task} is not always registered or cancellable (a create
     * point-in-time request runs on an unregistered {@link SearchTask}), so such a wait also needs a bound of its own.
     */
    void admit(Task task, ActionRequest request, ActionListener<Releasable> listener);

    /**
     * Runs {@code body} once {@code gate} admits the request, or directly, with {@code listener} itself, while the gate is
     * not {@linkplain #isActive() active}; run directly, {@code body}'s exceptions reach the caller as if there were no gate.
     * <p>
     * Once admitted, {@code body} gets a listener that closes the permit <em>before</em> notifying {@code listener}: a
     * completion handler may start the next request at once (an {@code _msearch} sends its next sub-search from the
     * previous one's response handler), and that request must find the permit returned. A failed close is logged, never
     * reported. A refused admission fails {@code listener} without running {@code body}. An exception thrown by
     * {@link #admit}, or by {@code body} after admission, fails {@code listener} (closing the permit first) instead of
     * reaching the caller, and an admitted request notifies {@code listener} at most once.
     *
     * @param gate     the node's gate
     * @param task     the request's task
     * @param request  the search or scroll request
     * @param listener notified once with the request's outcome, after its permit is closed
     * @param body     runs the request, completing the listener it is given
     */
    static <T> void execute(
        SearchExecutionGate gate,
        Task task,
        ActionRequest request,
        ActionListener<T> listener,
        Consumer<ActionListener<T>> body
    ) {
        if (gate.isActive() == false) {
            body.accept(listener);
            return;
        }
        final ActionListener<Releasable> permitListener = ActionListener.wrap(permit -> {
            assert permit != null : "a search execution gate must deliver a non-null permit";
            // notifyOnce: if body throws after completing its listener, the catch below must not notify or release again.
            final ActionListener<T> admitted = ActionListener.notifyOnce(ActionListener.runBefore(listener, () -> closePermit(permit)));
            try {
                body.accept(admitted);
            } catch (Exception e) {
                admitted.onFailure(e);
            }
        }, listener::onFailure);
        try {
            gate.admit(task, request, permitListener);
        } catch (Exception e) {
            // admit reports outcomes through its listener; an unexpected throw still fails the request, not the caller.
            listener.onFailure(e);
        }
    }

    private static void closePermit(Releasable permit) {
        try {
            permit.close();
        } catch (Exception e) {
            // A failed release must never turn a completed request into a failure.
            LogManager.getLogger(SearchExecutionGate.class).warn("Failed to release search execution permit", e);
        }
    }
}

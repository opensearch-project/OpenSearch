/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Runs each search or scroll page through the node's {@link SearchAdmission}: gets a permit, runs the request under it,
 * and closes it when the request completes. One per node, bound in {@code Node} and injected into the search actions.
 * Final so that every admission gets the same run-and-release handling; tests build one around a fake
 * {@link SearchAdmission} rather than mocking it.
 *
 * @opensearch.internal
 */
@InternalApi
public final class SearchExecutionGate {

    private static final Logger logger = LogManager.getLogger(SearchExecutionGate.class);

    /** A gate whose admission is never active, so every request runs directly. */
    public static final SearchExecutionGate NOOP = new SearchExecutionGate(new SearchAdmission() {
        @Override
        public boolean isActive() {
            return false;
        }

        @Override
        public void admit(Task task, ActionRequest request, ActionListener<Releasable> listener) {
            listener.onResponse(NOOP_PERMIT);
        }
    });

    private final SearchAdmission admission;

    public SearchExecutionGate(SearchAdmission admission) {
        this.admission = Objects.requireNonNull(admission);
    }

    /**
     * Runs {@code body} once the admission admits the request, or directly, with {@code listener} itself, while the
     * admission is not {@linkplain SearchAdmission#isActive() active}; run directly, {@code body}'s exceptions reach the
     * caller as if there were no gate.
     * <p>
     * Once admitted, {@code body} gets a listener that closes the permit <em>before</em> notifying {@code listener}: a
     * completion handler may start the next request at once (an {@code _msearch} sends its next sub-search from the
     * previous one's response handler), and that request must find the permit returned. A failed close is logged, never
     * reported. A refused admission fails {@code listener} without running {@code body}. An exception thrown by
     * {@link SearchAdmission#admit}, or by {@code body} after admission, fails {@code listener} (closing the permit first)
     * instead of reaching the caller, and an admitted request notifies {@code listener} at most once.
     *
     * @param task     the request's task
     * @param request  the search or scroll request
     * @param listener notified once with the request's outcome, after its permit is closed
     * @param body     runs the request, completing the listener it is given
     */
    public <T> void execute(Task task, ActionRequest request, ActionListener<T> listener, Consumer<ActionListener<T>> body) {
        if (admission.isActive() == false) {
            body.accept(listener);
            return;
        }
        final ActionListener<Releasable> permitListener = ActionListener.wrap(permit -> {
            assert permit != null : "a search admission must deliver a non-null permit";
            // notifyOnce: if body throws after completing its listener, the catch below must not notify or release again.
            final ActionListener<T> admitted = ActionListener.notifyOnce(ActionListener.runBefore(listener, () -> closePermit(permit)));
            try {
                body.accept(admitted);
            } catch (Exception e) {
                admitted.onFailure(e);
            }
        }, listener::onFailure);
        try {
            admission.admit(task, request, permitListener);
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
            logger.warn("Failed to release search execution permit", e);
        }
    }
}

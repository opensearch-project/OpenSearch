/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.exec.stage.StageExecution;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wires the child→parent state cascade for a parent execution and its child set.
 * On every child SUCCEEDED, decrement a shared counter; when it hits zero, start the
 * parent. On every child FAILED / CANCELLED, propagate to the parent via
 * {@link StageExecution#failFromChild(Exception)} (if a cause was captured) or
 * {@link StageExecution#cancel(String)} (otherwise).
 *
 * <p>Lifted out of {@link QueryExecution} so the recursive graph construction stays
 * focused on building stages and the cascade can be reasoned about / tested
 * independently. {@link QueryExecution} now calls {@link #wire} once per parent
 * during the recursive build.
 *
 * @opensearch.internal
 */
final class ChildToParentCascade {

    private ChildToParentCascade() {}

    /**
     * Attach a listener to each child that drives the cascade. Caller is responsible
     * for ordering: every child must already have its own state listeners attached
     * before this is called, since {@link StageExecution#addStateListener} fires in
     * registration order.
     */
    static void wire(StageExecution parent, List<? extends StageExecution> children) {
        if (children.isEmpty()) return;

        AtomicInteger pending = new AtomicInteger(children.size());
        for (StageExecution child : children) {
            child.addStateListener((from, to) -> {
                switch (to) {
                    case SUCCEEDED -> {
                        if (pending.decrementAndGet() == 0) {
                            parent.start();
                        }
                    }
                    case FAILED, CANCELLED -> {
                        Exception cause = child.getFailure();
                        if (cause != null) {
                            parent.failFromChild(cause);
                        } else {
                            parent.cancel("child " + child.getStageId() + " " + to);
                        }
                    }
                    default -> {
                    }
                }
            });
        }
    }
}

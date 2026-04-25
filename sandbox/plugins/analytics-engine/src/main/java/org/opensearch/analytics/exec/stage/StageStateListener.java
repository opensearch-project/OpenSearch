/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

/**
 * Observer for {@link StageExecution} state transitions. Fired synchronously
 * from within the execution's {@code transitionTo(...)} helper; listener
 * implementations must be non-blocking and exception-safe.
 *
 * <p>Listeners are registered during the walker's setup phase, before any
 * stage is started. Registration after {@code start()} is unsupported.
 *
 * @opensearch.internal
 */
public interface StageStateListener {

    /**
     * Called when a {@link StageExecution} transitions from one state to another.
     *
     * @param from the previous state
     * @param to   the new state
     */
    void onStateChange(StageExecution.State from, StageExecution.State to);
}

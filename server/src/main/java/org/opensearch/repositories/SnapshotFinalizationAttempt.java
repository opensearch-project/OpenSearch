/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.concurrent.atomic.AtomicReference;

/**
 * One snapshot finalization's outcome, shared by the caller that gave it a time budget and the repository that runs it.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class SnapshotFinalizationAttempt {

    private enum State {
        RUNNING,
        WRITING,
        ABANDONED,
        EXITED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);

    /** Caller: gives up on a finalization that has not started writing the repository generation; false once it has, or has returned. */
    public boolean abandon() {
        return state.compareAndSet(State.RUNNING, State.ABANDONED);
    }

    /** Repository: whether the caller has given up. */
    public boolean isAbandoned() {
        return state.get() == State.ABANDONED;
    }

    /**
     * Repository: called before it writes anything that makes the snapshot part of the repository. False means the caller
     * gave up first and nothing may be written; once true, the caller can no longer give up on the call.
     */
    public boolean startGenerationWrite() {
        return state.compareAndSet(State.RUNNING, State.WRITING) || state.get() == State.WRITING;
    }

    /** Caller: whether the finalization is writing the repository generation and has not returned. */
    public boolean isWritingGeneration() {
        return state.get() == State.WRITING;
    }

    /** Caller: claims the outcome when the call returns. False means the caller gave up first and already owns it. */
    public boolean exit() {
        return state.compareAndSet(State.RUNNING, State.EXITED) || state.compareAndSet(State.WRITING, State.EXITED);
    }
}

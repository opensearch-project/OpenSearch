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
 * Coordinates a caller's timeout with a repository's generation publication for one snapshot finalization. The caller
 * calls {@link #abandon()} when its timeout fires and {@link #exit()} when the repository call returns; at most one of
 * them succeeds, and that caller path completes the caller and releases the repository. The repository calls only
 * {@link #isAbandoned()} and {@link #startGenerationWrite()}.
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

    /**
     * Attempts to abandon the operation before generation publication is claimed.
     *
     * @return {@code true} if this call abandoned the operation; {@code false} if publication was claimed or the operation
     *         had already ended
     */
    public boolean abandon() {
        return state.compareAndSet(State.RUNNING, State.ABANDONED);
    }

    /** Returns whether the operation was abandoned before publication. */
    public boolean isAbandoned() {
        return state.get() == State.ABANDONED;
    }

    /**
     * Attempts to claim generation publication.
     *
     * @return {@code true} if publication was claimed by this or an earlier call; {@code false} after abandonment or completion
     */
    public boolean startGenerationWrite() {
        return state.compareAndSet(State.RUNNING, State.WRITING) || state.get() == State.WRITING;
    }

    /** Returns whether publication was claimed and the repository call remains active. */
    public boolean isWritingGeneration() {
        return state.get() == State.WRITING;
    }

    /**
     * Marks a running or publishing operation complete.
     *
     * @return {@code true} if this call completed the operation; {@code false} after abandonment or prior completion
     */
    public boolean exit() {
        return state.compareAndSet(State.RUNNING, State.EXITED) || state.compareAndSet(State.WRITING, State.EXITED);
    }
}

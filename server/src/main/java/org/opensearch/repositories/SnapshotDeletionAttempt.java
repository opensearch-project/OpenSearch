/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;

import java.util.concurrent.atomic.AtomicReference;

/**
 * One attempt at deleting snapshots, shared by a caller that may stop waiting and the worker that keeps running, because a
 * call blocked in an object store cannot be cancelled. The worker claims publication with {@link #claimCommit()} as the
 * commit's last step and reports it with {@link #committed} or {@link #commitUnconfirmed}; the caller's {@link #expire}
 * answers from what the worker has reached. A failure answer does not prove the deletion did not take effect: a claimed
 * commit whose publication failed may still take effect. Once expired, {@link #isAbandoned()} is {@code true} and the worker
 * begins no further destructive work; work already begun finishes, and what is skipped stays in the repository.
 * <p>
 * Each call into the repository gets its own instance: a deletion re-issued after being given up on reads the repository
 * afresh under a new attempt.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class SnapshotDeletionAttempt {

    /** What {@link #expire} found. */
    @ExperimentalApi
    public enum Expiry {
        /** No commit was confirmed; the caller answers failure. */
        NOT_COMMITTED,
        /** The commit was in flight: the continuation is answered once the publication reports an outcome. */
        PENDING,
        /** The commit was done: the continuation has been answered with the committed repository data. */
        RELEASED
    }

    private enum State {
        LIVE,
        /** The caller stopped waiting before the commit was claimed, or the claimed commit was not confirmed. */
        ABANDONED,
        COMMITTING,
        COMMITTING_EXPIRED,
        COMMITTED,
        RELEASED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.LIVE);
    private final AtomicReference<Exception> cleanupFailure = new AtomicReference<>();
    private volatile RepositoryData committed;
    private volatile ActionListener<RepositoryData> expiryContinuation;

    /** Returns a new attempt that nothing expires, for generation writes and deletes without a time budget. */
    public static SnapshotDeletionAttempt notAbandoned() {
        return new SnapshotDeletionAttempt();
    }

    /**
     * Attempts to claim generation publication as its last pre-publication step.
     *
     * @return {@code true} only if this call changes a live attempt to committing; publication must not follow {@code false}
     */
    public boolean claimCommit() {
        return state.compareAndSet(State.LIVE, State.COMMITTING);
    }

    /**
     * Reports that the claimed commit took effect with the given repository data, and answers an expiry that is waiting. Call it
     * at most once, and only after {@link #claimCommit()} returned {@code true}.
     */
    public void committed(RepositoryData data) {
        this.committed = data;
        while (true) {
            final State current = state.get();
            if (current == State.COMMITTING) {
                if (state.compareAndSet(current, State.COMMITTED)) {
                    return;
                }
            } else if (current == State.COMMITTING_EXPIRED) {
                if (state.compareAndSet(current, State.RELEASED)) {
                    expiryContinuation.onResponse(data);
                    return;
                }
            } else {
                assert false : "a commit reported in state " + current;
                return;
            }
        }
    }

    /**
     * Reports that the claimed commit was not confirmed, and fails an expiry that is waiting with the given cause. Call it at
     * most once, in place of {@link #committed}; it does nothing if no commit was claimed.
     */
    public void commitUnconfirmed(Exception e) {
        while (true) {
            final State current = state.get();
            if (current == State.COMMITTING) {
                if (state.compareAndSet(current, State.ABANDONED)) {
                    return;
                }
            } else if (current == State.COMMITTING_EXPIRED) {
                if (state.compareAndSet(current, State.ABANDONED)) {
                    expiryContinuation.onFailure(e);
                    return;
                }
            } else if (current == State.LIVE || current == State.ABANDONED) {
                return;
            } else {
                assert false : "a commit failure reported in state " + current;
                return;
            }
        }
    }

    /** Records the first cleanup failure. */
    public void recordCleanupFailure(Exception e) {
        cleanupFailure.compareAndSet(null, e);
    }

    /**
     * Records that the caller has stopped waiting and returns how it is answered, as {@link Expiry} describes; on
     * {@link Expiry#NOT_COMMITTED} the continuation is never called. The continuation is recorded before the state is read, so
     * a second call made while an earlier call's {@link Expiry#PENDING} continuation is outstanding replaces that continuation.
     *
     * @throws IllegalStateException if the attempt was already expired and its claimed commit is still in flight or took effect
     */
    public Expiry expire(ActionListener<RepositoryData> continuation) {
        this.expiryContinuation = continuation;
        while (true) {
            final State current = state.get();
            switch (current) {
                case LIVE:
                    if (state.compareAndSet(current, State.ABANDONED)) {
                        return Expiry.NOT_COMMITTED;
                    }
                    break;
                case ABANDONED:
                    return Expiry.NOT_COMMITTED;
                case COMMITTING:
                    if (state.compareAndSet(current, State.COMMITTING_EXPIRED)) {
                        return Expiry.PENDING;
                    }
                    break;
                case COMMITTED:
                    if (state.compareAndSet(current, State.RELEASED)) {
                        continuation.onResponse(committed);
                        return Expiry.RELEASED;
                    }
                    break;
                default:
                    throw new IllegalStateException("attempt expired twice, in state " + current);
            }
        }
    }

    /**
     * Returns whether the attempt expired or publication was unconfirmed. Use one result for operations that must be skipped
     * together.
     */
    public boolean isAbandoned() {
        final State current = state.get();
        return current == State.ABANDONED || current == State.COMMITTING_EXPIRED || current == State.RELEASED;
    }

    /** The repository data the commit took effect with, or {@code null} if it has not been reported. */
    @Nullable
    public RepositoryData committedRepositoryData() {
        return committed;
    }

    /** Returns the first cleanup failure, or {@code null}. */
    @Nullable
    public Exception cleanupFailure() {
        return cleanupFailure.get();
    }

    @Override
    public String toString() {
        return "SnapshotDeletionAttempt[" + state.get() + ']';
    }
}

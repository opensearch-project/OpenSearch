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
 * One attempt at deleting snapshots from a repository, shared by the caller that may stop waiting for it and the worker that
 * runs it.
 * <p>
 * A caller that puts a time budget on a delete stops waiting when the budget expires, but the worker it started keeps
 * running, because a call already blocked in an object store cannot be cancelled. The worker claims the generation commit
 * with {@link #claimCommit()} as the commit's last step, and reports its outcome with {@link #committed} or
 * {@link #commitUnconfirmed}. The caller's {@link #expire} then decides the answer from what the worker has reached: no commit
 * confirmed, so the caller answers failure (a commit not yet claimed is then refused; one claimed whose publication failed may
 * still take effect); the commit in flight, so the answer waits for its outcome; or the commit done, so the caller answers
 * success with the committed repository data.
 * <p>
 * Once the caller has expired the attempt, {@link #isAbandoned()} is {@code true} and the worker begins no further piece of
 * destructive work; a piece already begun is allowed to finish. What the worker then does not do is left in the
 * repository.
 * <p>
 * One instance belongs to one call into the repository, not to one snapshot deletion. A deletion that is re-issued after
 * being given up on is a second call, reading the repository afresh, and it gets its own instance.
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
        /** The caller is waiting and the worker has not claimed the commit. */
        LIVE,
        /** The caller stopped waiting before the commit was claimed, or the claimed commit was not confirmed. */
        ABANDONED,
        /** The worker claimed the commit and its outcome is not yet known. */
        COMMITTING,
        /** The caller stopped waiting while the commit was in flight. */
        COMMITTING_EXPIRED,
        /** The commit took effect and the caller is still waiting. */
        COMMITTED,
        /** The commit took effect and the caller has stopped waiting. */
        RELEASED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.LIVE);
    private final AtomicReference<Exception> cleanupFailure = new AtomicReference<>();
    private volatile RepositoryData committed;
    private volatile ActionListener<RepositoryData> expiryContinuation;

    /**
     * An attempt no caller can expire, for generation writes and deletes with no time budget. Each call gets its own
     * instance, so no caller can expire another's.
     */
    public static SnapshotDeletionAttempt notAbandoned() {
        return new SnapshotDeletionAttempt();
    }

    /**
     * Claims the generation commit, as the commit's last step. Returns {@code false} if the caller has already stopped
     * waiting, in which case the commit must not be made.
     */
    public boolean claimCommit() {
        return state.compareAndSet(State.LIVE, State.COMMITTING);
    }

    /** Reports that the claimed commit took effect with the given repository data, and answers an expiry that is waiting. */
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

    /** Reports that the claimed commit was not confirmed, and fails an expiry that is waiting with the given cause. */
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
                // Nothing was claimed: the commit task failed before its last step, or it refused the claim.
                return;
            } else {
                assert false : "a commit failure reported in state " + current;
                return;
            }
        }
    }

    /** Records a failure of a cleanup step of this attempt. The first one is kept. */
    public void recordCleanupFailure(Exception e) {
        cleanupFailure.compareAndSet(null, e);
    }

    /**
     * Records that the caller has stopped waiting, and says how it is to be answered. On {@link Expiry#PENDING} the given
     * continuation is answered once the publication reports an outcome, and on {@link Expiry#RELEASED} it has been answered before
     * this returns; on {@link Expiry#NOT_COMMITTED} it is never called.
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
     * Whether the caller has stopped waiting, or the claimed commit was not confirmed. Read once per unit of destructive
     * work and reuse that one answer for every guard within the unit, so that guards which are only safe to skip together
     * cannot disagree.
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

    /** The first cleanup failure recorded, or {@code null}. */
    @Nullable
    public Exception cleanupFailure() {
        return cleanupFailure.get();
    }

    @Override
    public String toString() {
        return "SnapshotDeletionAttempt[" + state.get() + ']';
    }
}

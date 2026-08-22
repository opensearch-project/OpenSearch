/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks translog bytes written since the last successful index commit.
 *
 * @opensearch.internal
 */
final class TranslogBytesTracker {

    private final AtomicLong bytesSinceLastCommit = new AtomicLong();
    private final AtomicBoolean initialized = new AtomicBoolean();

    /**
     * Seeds the tracker with the translog bytes that were already written beyond the last commit before this tracker
     * existed. Only the first call takes effect, so callers can invoke this from every entry point without guarding.
     *
     * @param baselineBytes uncommitted translog bytes present at the time tracking starts
     * @return {@code true} if this call seeded the tracker
     */
    boolean initialize(long baselineBytes) {
        if (baselineBytes < 0) {
            throw new IllegalArgumentException("translog bytes must be non-negative");
        }
        if (initialized.compareAndSet(false, true) == false) {
            return false;
        }
        addBytes(baselineBytes);
        return true;
    }

    boolean isInitialized() {
        return initialized.get();
    }

    void addBytes(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("translog bytes must be non-negative");
        }
        /*
         * Saturate rather than overflow. Both operands are non-negative here, so a negative sum can only mean the
         * counter wrapped. This runs on the indexing path and only feeds a flush decision, so it must never throw and
         * fail the shard over an accounting detail.
         */
        bytesSinceLastCommit.updateAndGet(current -> current + bytes < 0 ? Long.MAX_VALUE : current + bytes);
    }

    long getBytesSinceLastCommit() {
        return bytesSinceLastCommit.get();
    }

    CommitSnapshot startCommit() {
        return new CommitSnapshot(bytesSinceLastCommit.get());
    }

    void completeCommit(CommitSnapshot commitSnapshot) {
        bytesSinceLastCommit.updateAndGet(current -> {
            if (commitSnapshot.bytes > current) {
                throw new IllegalStateException(
                    "commit snapshot contains [" + commitSnapshot.bytes + "] bytes but only [" + current + "] bytes are tracked"
                );
            }
            return current - commitSnapshot.bytes;
        });
    }

    static final class CommitSnapshot {
        private final long bytes;

        private CommitSnapshot(long bytes) {
            this.bytes = bytes;
        }
    }
}

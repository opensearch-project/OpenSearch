/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks translog bytes written since the last successful index commit.
 *
 * @opensearch.internal
 */
final class TranslogBytesTracker {

    private final AtomicLong bytesSinceLastCommit = new AtomicLong();

    void addBytes(long bytes) {
        if (bytes < 0) {
            throw new IllegalArgumentException("translog bytes must be non-negative");
        }
        bytesSinceLastCommit.updateAndGet(current -> Math.addExact(current, bytes));
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

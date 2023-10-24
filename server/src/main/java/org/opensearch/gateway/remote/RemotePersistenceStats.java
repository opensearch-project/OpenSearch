/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.coordination.PersistedStateStats;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Remote state related extended stats.
 *
 * @opensearch.internal
 */
public class RemotePersistenceStats extends PersistedStateStats {
    static final String CLEANUP_ATTEMPT_FAILED_COUNT = "cleanup_attempt_failed_count";
    static final String REMOTE_UPLOAD = "remote_upload";
    private AtomicLong cleanupAttemptFailedCount = new AtomicLong(0);

    public RemotePersistenceStats() {
        super(REMOTE_UPLOAD);
        addToExtendedFields(CLEANUP_ATTEMPT_FAILED_COUNT, cleanupAttemptFailedCount);
    }

    public void cleanUpAttemptFailed() {
        cleanupAttemptFailedCount.incrementAndGet();
    }

    public long getCleanupAttemptFailedCount() {
        return cleanupAttemptFailedCount.get();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.repositories.RepositoryException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-repository guard bounding the number of outstanding cluster-manager-side repository operations, so a degraded
 * repository cannot drain the shared snapshot thread pool. Past the limit, {@link #tryAcquire(String)} fails fast
 * instead of dispatching another operation.
 * @opensearch.internal
 */
public final class RepositoryOperationGuard {
    private final Map<String, Integer> outstandingOpsByRepository = new ConcurrentHashMap<>();
    private volatile int maxOutstandingOps;

    public RepositoryOperationGuard(int maxOutstandingOps) {
        setMaxOutstandingOps(maxOutstandingOps);
    }

    public void setMaxOutstandingOps(int maxOutstandingOps) {
        if (maxOutstandingOps <= 0) {
            throw new IllegalArgumentException("maxOutstandingOps must be positive, got [" + maxOutstandingOps + "]");
        }
        this.maxOutstandingOps = maxOutstandingOps;
    }

    public int getMaxOutstandingOps() {
        return maxOutstandingOps;
    }

    /**
     * Acquires a permit for {@code repository}, or throws {@link RepositoryException} if the limit is reached. On
     * success the caller must call {@link #release(String)} exactly once for this acquisition.
     */
    public void tryAcquire(String repository) {
        final int limit = maxOutstandingOps;
        outstandingOpsByRepository.compute(repository, (repo, current) -> {
            final int count = (current == null) ? 0 : current;
            if (count >= limit) {
                throw new RepositoryException(repo, "[" + count + "] outstanding repository operations at the limit of [" + limit + "]");
            }
            return count + 1;
        });
    }

    /**
     * Releases a permit for {@code repository}. Must be called exactly once per successful {@link #tryAcquire(String)}.
     */
    public void release(String repository) {
        outstandingOpsByRepository.compute(repository, (repo, current) -> {
            if (current == null || current <= 0) {
                assert false : "release() with no outstanding operations tracked for repository [" + repo + "]";
                return null;
            }
            final int updated = current - 1;
            return updated == 0 ? null : updated;
        });
    }

    public int getOutstandingOps(String repository) {
        return outstandingOpsByRepository.getOrDefault(repository, 0);
    }
}

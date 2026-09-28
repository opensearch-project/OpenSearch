/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Tracks thread-level resource attribution for delegation callbacks executing
 * on foreign threads (e.g., DataFusion/Tokio workers invoking Lucene via FFM).
 *
 * @opensearch.internal
 */
public interface DelegationThreadTracker {

    /**
     * Signal that delegation work has started on the current thread.
     *
     * @return thread id to pass to {@link #trackEnd}, or {@code -1} if tracking is inactive
     */
    long trackStart();

    /**
     * Signal that delegation work has finished on the given thread.
     *
     * @param threadId the value returned by {@link #trackStart}
     */
    void trackEnd(long threadId);
}

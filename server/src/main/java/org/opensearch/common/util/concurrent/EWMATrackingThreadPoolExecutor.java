/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

/**
 * Marks the thread pool executor as supporting EWMA (exponential weighted moving average) tracking
 *
 * @opensearch.internal
 */
public interface EWMATrackingThreadPoolExecutor {
    /**
     * This is a random starting point alpha
     */
    double EWMA_ALPHA = 0.3;

    /**
     * Returns the exponentially weighted moving average of the task execution time
     */
    double getTaskExecutionEWMA();

    /**
     * Returns the current queue size (operations that are queued)
     */
    int getCurrentQueueSize();
}

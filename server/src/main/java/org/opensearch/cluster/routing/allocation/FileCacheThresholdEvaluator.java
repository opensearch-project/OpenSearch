/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats;

/**
 * Base interface for file cache threshold evaluation logic.
 * This interface defines methods for evaluating whether a node exceeds
 * various watermarks based on file cache active usage.
 *
 * @opensearch.internal
 */
public interface FileCacheThresholdEvaluator {

    /**
     * Checks if a node is exceeding the high watermark threshold
     *
     * @param aggregateFileCacheStats disk usage for the node
     * @return true if the node is exceeding the high watermark, false otherwise
     */
    boolean isNodeExceedingHighWatermark(AggregateFileCacheStats aggregateFileCacheStats);

    /**
     * Checks if a node is exceeding the flood stage watermark threshold
     *
     * @param aggregateFileCacheStats disk usage for the node
     * @return true if the node is exceeding the flood stage watermark, false otherwise
     */
    boolean isNodeExceedingFloodStageWatermark(AggregateFileCacheStats aggregateFileCacheStats);

}

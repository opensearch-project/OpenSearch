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
 * Evaluates file cache active usage thresholds for warm nodes in the cluster.
 * This class provides methods to check if nodes are exceeding various file cache watermark levels
 */
public class FileCacheEvaluator implements FileCacheThresholdEvaluator {

    private final FileCacheThresholdSettings fileCacheThresholdSettings;

    public FileCacheEvaluator(FileCacheThresholdSettings fileCacheThresholdSettings) {
        this.fileCacheThresholdSettings = fileCacheThresholdSettings;
    }

    @Override
    public boolean isNodeExceedingIndexingThreshold(AggregateFileCacheStats aggregateFileCacheStats) {
        return aggregateFileCacheStats.getOverallActivePercent() >= fileCacheThresholdSettings.getFreeFileCacheIndexThreshold();
    }

    @Override
    public boolean isNodeExceedingSearchThreshold(AggregateFileCacheStats aggregateFileCacheStats) {
        return aggregateFileCacheStats.getOverallActivePercent() >= fileCacheThresholdSettings.getFreeFileCacheSearchThreshold();
    }
}

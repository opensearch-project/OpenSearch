/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats;

/**
 * Evaluates file cache active usage thresholds for warm nodes in the cluster.
 * This class provides methods to check if nodes are exceeding various file cache watermark levels
 */
public class FileCacheEvaluator {

    private final FileCacheThresholdSettings fileCacheThresholdSettings;

    public FileCacheEvaluator(FileCacheThresholdSettings fileCacheThresholdSettings) {
        this.fileCacheThresholdSettings = fileCacheThresholdSettings;
    }

    public boolean isNodeExceedingIndexingThreshold(AggregateFileCacheStats aggregateFileCacheStats) {
        if (fileCacheThresholdSettings.isEnabled() == false) return false;
        else if (fileCacheThresholdSettings.getFileCacheIndexThresholdPercentage().equals(0.0) == false) {
            return aggregateFileCacheStats.getOverallActivePercent() >= fileCacheThresholdSettings.getFileCacheIndexThresholdPercentage();
        } else if (fileCacheThresholdSettings.getFileCacheIndexThresholdBytes().equals(ByteSizeValue.ZERO) == false) {
            return aggregateFileCacheStats.getActive().getBytes() >= fileCacheThresholdSettings.getFileCacheIndexThresholdBytes()
                .getBytes();
        }
        return false;
    }

    public boolean isNodeExceedingSearchThreshold(AggregateFileCacheStats aggregateFileCacheStats) {
        if (fileCacheThresholdSettings.isEnabled() == false) return false;
        else if (fileCacheThresholdSettings.getFileCacheSearchThresholdPercentage().equals(0.0) == false) {
            return aggregateFileCacheStats.getOverallActivePercent() >= fileCacheThresholdSettings.getFileCacheSearchThresholdPercentage();
        } else if (fileCacheThresholdSettings.getFileCacheSearchThresholdBytes().equals(ByteSizeValue.ZERO) == false) {
            return aggregateFileCacheStats.getActive().getBytes() >= fileCacheThresholdSettings.getFileCacheSearchThresholdBytes()
                .getBytes();
        }
        return false;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.metrics.CounterMetric;

/**
 * Collector for full file cache stats.
 */
@PublicApi(since = "3.0.0")
public class FullFileCacheStatsCollector {
    private final CounterMetric activeFullFileBytes = new CounterMetric();
    private final CounterMetric activeFullFileCount = new CounterMetric();
    private final CounterMetric usedFullFileBytes = new CounterMetric();
    private final CounterMetric usedFullFileCount = new CounterMetric();
    private final CounterMetric evictedFullFileBytes = new CounterMetric();
    private final CounterMetric evictedFullFileCount = new CounterMetric();
    private final CounterMetric hitCount = new CounterMetric();
    private final CounterMetric missCount = new CounterMetric();

    public void recordUsedFileBytes(long bytes, boolean increment) {
        if (increment) {
            usedFullFileBytes.inc(bytes);
            usedFullFileCount.inc();
        } else {
            usedFullFileBytes.dec(bytes);
            usedFullFileCount.dec();
            evictedFullFileBytes.inc(bytes);
            evictedFullFileCount.inc();
        }
    }

    public void recordActiveFileBytes(long bytes, boolean increment) {
        if (increment) {
            activeFullFileBytes.inc(bytes);
            activeFullFileCount.inc();
        } else {
            activeFullFileBytes.dec(bytes);
            activeFullFileCount.dec();
        }
    }

    public void recordHit(boolean hit) {
        if (hit) {
            hitCount.inc();
        } else {
            missCount.inc();
        }
    }

    public FullFileCacheStats getStats() {
        return new FullFileCacheStats(
            activeFullFileBytes.count(),
            activeFullFileCount.count(),
            usedFullFileBytes.count(),
            usedFullFileCount.count(),
            evictedFullFileBytes.count(),
            evictedFullFileCount.count(),
            hitCount.count(),
            missCount.count()
        );
    }
}

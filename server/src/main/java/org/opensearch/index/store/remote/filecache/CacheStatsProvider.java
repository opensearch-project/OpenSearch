/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * SPI contract for any cache that exposes stats for the unified stats aggregation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface CacheStatsProvider {

    /**
     * Returns a point-in-time snapshot of this cache's statistics.
     */
    NodeCacheStats cacheStats();
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import java.util.List;

/**
 * An interface extended by DefaultCacheStatsHolder and NoopCacheStatsHolder.
 */
public interface CacheStatsHolder {
    void incrementHits(List<String> dimensionValues);

    void incrementMisses(List<String> dimensionValues);

    void incrementEvictions(List<String> dimensionValues);

    void incrementSizeInBytes(List<String> dimensionValues, long amountBytes);

    void decrementSizeInBytes(List<String> dimensionValues, long amountBytes);

    void incrementItems(List<String> dimensionValues);

    void decrementItems(List<String> dimensionValues);

    void reset();

    long count();

    void removeDimensions(List<String> dimensionValues);

    ImmutableCacheStatsHolder getImmutableCacheStatsHolder(String[] levels);
}

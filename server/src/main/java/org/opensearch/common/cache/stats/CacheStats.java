/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.core.common.io.stream.Writeable;

import java.util.List;

/**
 * Interface for any cache specific stats. Allows accessing stats by total value or by dimension,
 * and also allows updating stats.
 * When updating stats, we take in the list of dimensions associated with the key/value pair that caused the update.
 * This allows us to aggregate stats by dimension when accessing them.
 */
public interface CacheStats extends Writeable {

    // Methods to get all 5 values at once, either in total or for a specific set of dimensions.
    CacheStatsResponse getTotalStats();

    CacheStatsResponse getStatsByDimensions(List<CacheStatsDimension> dimensions);

    // Methods to get total values.
    long getTotalHits();

    long getTotalMisses();

    long getTotalEvictions();

    long getTotalMemorySize();

    long getTotalEntries();

    // Methods to get values for a specific set of dimensions.
    // Returns the sum of values for cache entries that match all dimensions in the list.
    long getHitsByDimensions(List<CacheStatsDimension> dimensions);

    long getMissesByDimensions(List<CacheStatsDimension> dimensions);

    long getEvictionsByDimensions(List<CacheStatsDimension> dimensions);

    long getMemorySizeByDimensions(List<CacheStatsDimension> dimensions);

    long getEntriesByDimensions(List<CacheStatsDimension> dimensions);

    void incrementHitsByDimensions(List<CacheStatsDimension> dimensions);

    void incrementMissesByDimensions(List<CacheStatsDimension> dimensions);

    void incrementEvictionsByDimensions(List<CacheStatsDimension> dimensions);

    // Can also use to decrement, with negative values
    void incrementMemorySizeByDimensions(List<CacheStatsDimension> dimensions, long amountBytes);

    void incrementEntriesByDimensions(List<CacheStatsDimension> dimensions);

    void decrementEntriesByDimensions(List<CacheStatsDimension> dimensions);

    // Resets memory and entries stats but leaves the others; called when the cache clears itself.
    void reset();

}

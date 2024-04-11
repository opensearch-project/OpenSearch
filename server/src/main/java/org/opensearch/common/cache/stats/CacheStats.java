/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Interface for access to any cache stats. Allows accessing stats by dimension values.
 * Stores an immutable snapshot of stats for a cache. The cache maintains its own live counters.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface CacheStats { // TODO: also extends Writeable, ToXContentFragment (in API PR)

    // Method to get all 5 values at once
    CacheStatsCounterSnapshot getTotalStats();

    // Methods to get total values.
    long getTotalHits();

    long getTotalMisses();

    long getTotalEvictions();

    long getTotalSizeInBytes();

    long getTotalEntries();
}

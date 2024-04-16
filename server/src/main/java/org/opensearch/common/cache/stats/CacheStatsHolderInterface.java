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
 * An abstract class extended by CacheStatsHolder and DummyCacheStatsHolder.
 * Can be removed once the pluggable caches feature is no longer experimental.
 */
public interface CacheStatsHolderInterface {
    void incrementHits(List<String> dimensionValues);

    void incrementMisses(List<String> dimensionValues);

    void incrementEvictions(List<String> dimensionValues);

    void incrementSizeInBytes(List<String> dimensionValues, long amountBytes);

    void decrementSizeInBytes(List<String> dimensionValues, long amountBytes);

    void incrementEntries(List<String> dimensionValues);

    void decrementEntries(List<String> dimensionValues);

    void reset();

    long count();

    void removeDimensions(List<String> dimensionValues);

    ImmutableCacheStatsHolder getImmutableCacheStatsHolder();
}

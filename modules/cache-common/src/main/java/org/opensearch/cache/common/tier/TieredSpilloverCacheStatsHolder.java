/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.tier;

import org.opensearch.common.cache.stats.DefaultCacheStatsHolder;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * A tier-aware version of DefaultCacheStatsHolder. Overrides the incrementer functions, as we can't just add the on-heap
 * and disk stats to get a total for the cache as a whole. If the disk tier is present, the total hits, size, and entries
 * should be the sum of both tiers' values, but the total misses and evictions should be the disk tier's values.
 * When the disk tier isn't present, on-heap misses and evictions should contribute to the total.
 *
 * For example, if the heap tier has 5 misses and the disk tier has 4, the total cache has had 4 misses, not 9.
 * The same goes for evictions. Other stats values add normally.
 *
 * This means for misses and evictions, if we are incrementing for the on-heap tier and the disk tier is present,
 * we have to increment only the leaf nodes corresponding to the on-heap tier itself, and not its ancestors,
 * which correspond to totals including both tiers. If the disk tier is not present, we do increment the ancestor nodes.
 */
public class TieredSpilloverCacheStatsHolder extends DefaultCacheStatsHolder {

    /** Whether the disk cache is currently enabled. */
    private boolean diskCacheEnabled;

    // Common values used for tier dimension

    /** The name for the tier dimension. */
    public static final String TIER_DIMENSION_NAME = "tier";

    /** Dimension value for on-heap cache, like OpenSearchOnHeapCache.*/
    public static final String TIER_DIMENSION_VALUE_ON_HEAP = "on_heap";

    /** Dimension value for on-disk cache, like EhcacheDiskCache. */
    public static final String TIER_DIMENSION_VALUE_DISK = "disk";

    /**
     * Constructor for the stats holder.
     * @param originalDimensionNames the original dimension names, not including TIER_DIMENSION_NAME
     * @param diskCacheEnabled whether the disk tier starts out enabled
     */
    public TieredSpilloverCacheStatsHolder(List<String> originalDimensionNames, boolean diskCacheEnabled) {
        super(
            getDimensionNamesWithTier(originalDimensionNames),
            TieredSpilloverCache.TieredSpilloverCacheFactory.TIERED_SPILLOVER_CACHE_NAME
        );
        this.diskCacheEnabled = diskCacheEnabled;
    }

    private static List<String> getDimensionNamesWithTier(List<String> dimensionNames) {
        List<String> dimensionNamesWithTier = new ArrayList<>(dimensionNames);
        dimensionNamesWithTier.add(TIER_DIMENSION_NAME);
        return dimensionNamesWithTier;
    }

    /**
     * Add tierValue to the end of a copy of the initial dimension values, so they can appropriately be used in this stats holder.
     */
    List<String> getDimensionsWithTierValue(List<String> initialDimensions, String tierValue) {
        List<String> result = new ArrayList<>(initialDimensions);
        result.add(tierValue);
        return result;
    }

    private String validateTierDimensionValue(List<String> dimensionValues) {
        String tierDimensionValue = dimensionValues.get(dimensionValues.size() - 1);
        assert tierDimensionValue.equals(TIER_DIMENSION_VALUE_ON_HEAP) || tierDimensionValue.equals(TIER_DIMENSION_VALUE_DISK)
            : "Invalid tier dimension value";
        return tierDimensionValue;
    }

    @Override
    public void incrementHits(List<String> dimensionValues) {
        validateTierDimensionValue(dimensionValues);
        // Hits from either tier should be included in the total values.
        super.incrementHits(dimensionValues);
    }

    @Override
    public void incrementMisses(List<String> dimensionValues) {
        final String tierValue = validateTierDimensionValue(dimensionValues);

        // If the disk tier is present, only misses from the disk tier should be included in total values.
        Consumer<Node> missIncrementer = (node) -> {
            if (tierValue.equals(TIER_DIMENSION_VALUE_ON_HEAP) && diskCacheEnabled) {
                // If on-heap tier, increment only the leaf node corresponding to the on heap values; not the total values in its parent
                // nodes
                if (node.isAtLowestLevel()) {
                    node.incrementMisses();
                }
            } else {
                // If disk tier, or on-heap tier with a disabled disk tier, increment the leaf node and its parents
                node.incrementMisses();
            }
        };
        internalIncrement(dimensionValues, missIncrementer, true);
    }

    /**
     * This method shouldn't be used in this class. Instead, use incrementEvictions(dimensionValues, includeInTotal)
     * which specifies whether the eviction should be included in the cache's total evictions, or if it should
     * just count towards that tier's evictions.
     * @param dimensionValues The dimension values
     */
    @Override
    public void incrementEvictions(List<String> dimensionValues) {
        throw new UnsupportedOperationException(
            "TieredSpilloverCacheHolder must specify whether to include an eviction in the total cache stats. Use incrementEvictions(List<String> dimensionValues, boolean includeInTotal)"
        );
    }

    /**
     * Increment evictions for this set of dimension values.
     * @param dimensionValues The dimension values
     * @param includeInTotal Whether to include this eviction in the total for the whole cache's evictions
     */
    public void incrementEvictions(List<String> dimensionValues, boolean includeInTotal) {
        validateTierDimensionValue(dimensionValues);
        // If we count this eviction towards the total, we should increment all ancestor nodes. If not, only increment the leaf node.
        Consumer<DefaultCacheStatsHolder.Node> evictionsIncrementer = (node) -> {
            if (includeInTotal || node.isAtLowestLevel()) {
                node.incrementEvictions();
            }
        };
        internalIncrement(dimensionValues, evictionsIncrementer, true);
    }

    @Override
    public void incrementSizeInBytes(List<String> dimensionValues, long amountBytes) {
        validateTierDimensionValue(dimensionValues);
        // Size from either tier should be included in the total values.
        super.incrementSizeInBytes(dimensionValues, amountBytes);
    }

    // For decrements, we should not create nodes if they are absent. This protects us from erroneously decrementing values for keys
    // which have been entirely deleted, for example in an async removal listener.
    @Override
    public void decrementSizeInBytes(List<String> dimensionValues, long amountBytes) {
        validateTierDimensionValue(dimensionValues);
        // Size from either tier should be included in the total values.
        super.decrementSizeInBytes(dimensionValues, amountBytes);
    }

    @Override
    public void incrementItems(List<String> dimensionValues) {
        validateTierDimensionValue(dimensionValues);
        // Entries from either tier should be included in the total values.
        super.incrementItems(dimensionValues);
    }

    @Override
    public void decrementItems(List<String> dimensionValues) {
        validateTierDimensionValue(dimensionValues);
        // Entries from either tier should be included in the total values.
        super.decrementItems(dimensionValues);
    }

    void setDiskCacheEnabled(boolean diskCacheEnabled) {
        this.diskCacheEnabled = diskCacheEnabled;
    }
}

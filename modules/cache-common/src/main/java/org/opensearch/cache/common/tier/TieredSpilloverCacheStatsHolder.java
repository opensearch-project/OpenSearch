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
import java.util.function.BiConsumer;

/**
 * A tier-aware version of DefaultCacheStatsHolder. Overrides the incrementer functions, as we cannot just add the on-heap
 * and disk stats to get a total for the cache as a whole. For example, if the heap tier has 5 misses and the disk tier
 * has 4, the total cache has had 4 misses, not 9. The same goes for evictions. Other stats values add normally.
 * This means for misses and evictions, if we are incrementing for the on-heap tier, we have to increment only the leaf nodes
 * corresponding to the on-heap tier itself, and not its ancestors.
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

    public TieredSpilloverCacheStatsHolder(List<String> originalDimensionNames, boolean diskCacheEnabled) {
        super(getDimensionNamesWithTier(originalDimensionNames));
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

    private String getAndCheckTierDimensionValue(List<String> dimensionValues) {
        String tierDimensionValue = dimensionValues.get(dimensionValues.size() - 1);
        assert tierDimensionValue.equals(TIER_DIMENSION_VALUE_ON_HEAP) || tierDimensionValue.equals(TIER_DIMENSION_VALUE_DISK)
            : "Invalid tier dimension value";
        return tierDimensionValue;
    }

    private boolean isLeafNode(int depth) {
        return depth == dimensionNames.size(); // Not size - 1, because there is also a root node
    }

    @Override
    public void incrementHits(List<String> dimensionValues) {
        getAndCheckTierDimensionValue(dimensionValues);
        // Hits from either tier should be included in the total values.
        internalIncrement(dimensionValues, (node, depth) -> node.incrementHits(), true);
    }

    @Override
    public void incrementMisses(List<String> dimensionValues) {
        final String tierValue = getAndCheckTierDimensionValue(dimensionValues);

        // If the disk tier is present, only misses from the disk tier should be included in total values.
        BiConsumer<Node, Integer> missIncrementer = (node, depth) -> {
            if (tierValue.equals(TIER_DIMENSION_VALUE_ON_HEAP) && diskCacheEnabled) {
                // If on-heap tier, increment only the leaf node corresponding to the on heap values; not the total values in its parent
                // nodes
                if (isLeafNode(depth)) {
                    node.incrementMisses();
                }
            } else {
                // If disk tier, or on-heap tier with a disabled disk tier, increment the leaf node and its parents
                node.incrementMisses();
            }
        };

        internalIncrement(dimensionValues, missIncrementer, true);
    }

    @Override
    public void incrementEvictions(List<String> dimensionValues) {
        final String tierValue = getAndCheckTierDimensionValue(dimensionValues);

        // If the disk tier is present, only evictions from the disk tier should be included in total values.
        BiConsumer<DefaultCacheStatsHolder.Node, Integer> evictionsIncrementer = (node, depth) -> {
            if (tierValue.equals(TIER_DIMENSION_VALUE_ON_HEAP) && diskCacheEnabled) {
                // If on-heap tier, increment only the leaf node corresponding to the on heap values; not the total values in its parent
                // nodes
                if (isLeafNode(depth)) {
                    node.incrementEvictions();
                }
            } else {
                // If disk tier, or on-heap tier with a disabled disk tier, increment the leaf node and its parents
                node.incrementEvictions();
            }
        };

        internalIncrement(dimensionValues, evictionsIncrementer, true);
    }

    @Override
    public void incrementSizeInBytes(List<String> dimensionValues, long amountBytes) {
        getAndCheckTierDimensionValue(dimensionValues);
        // Size from either tier should be included in the total values.
        internalIncrement(dimensionValues, (node, depth) -> node.incrementSizeInBytes(amountBytes), true);
    }

    // For decrements, we should not create nodes if they are absent. This protects us from erroneously decrementing values for keys
    // which have been entirely deleted, for example in an async removal listener.
    @Override
    public void decrementSizeInBytes(List<String> dimensionValues, long amountBytes) {
        getAndCheckTierDimensionValue(dimensionValues);
        // Size from either tier should be included in the total values.
        internalIncrement(dimensionValues, (node, depth) -> node.decrementSizeInBytes(amountBytes), false);
    }

    @Override
    public void incrementEntries(List<String> dimensionValues) {
        getAndCheckTierDimensionValue(dimensionValues);
        // Entries from either tier should be included in the total values.
        internalIncrement(dimensionValues, (node, depth) -> node.incrementEntries(), true);
    }

    @Override
    public void decrementEntries(List<String> dimensionValues) {
        getAndCheckTierDimensionValue(dimensionValues);
        // Entries from either tier should be included in the total values.
        internalIncrement(dimensionValues, (node, depth) -> node.decrementEntries(), false);
    }

    void setDiskCacheEnabled(boolean diskCacheEnabled) {
        this.diskCacheEnabled = diskCacheEnabled;
    }
}

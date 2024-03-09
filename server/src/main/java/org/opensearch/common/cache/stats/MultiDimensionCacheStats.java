/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * A CacheStats object supporting aggregation over multiple different dimensions.
 * Also keeps track of a tier dimension, which is the same for all values in the stats object.
 * Does not allow changes to the stats.
 */
public class MultiDimensionCacheStats implements CacheStats {

    // The value of the tier dimension for entries in this Stats object. This is handled separately for efficiency,
    // as it always has the same value for every entry in the stats object.
    // Package-private for testing.
    final String tierDimensionValue;

    // A StatsHolder containing stats maintained by the cache.
    // Pkg-private for testing.
    final StatsHolder statsHolder;

    public MultiDimensionCacheStats(StatsHolder statsHolder, String tierDimensionValue) {
        this.statsHolder = statsHolder;
        this.tierDimensionValue = tierDimensionValue;
    }

    public MultiDimensionCacheStats(StreamInput in) throws IOException {
        this.tierDimensionValue = in.readString();
        this.statsHolder = new StatsHolder(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(tierDimensionValue);
        statsHolder.writeTo(out);
    }

    @Override
    public CacheStatsResponse getTotalStats() {
        CacheStatsResponse response = new CacheStatsResponse();
        response.add(statsHolder.getTotalStats()); // Return a copy to prevent consumers of this method from changing the original
        return response;
    }

    /**
     * Get the stats response aggregated by dimensions. If there are no values for the specified dimensions,
     * returns an all-zero response. If the specified dimensions don't form a valid key, as determined by the statsHolder's
     * tracking mode, throws an IllegalArgumentException.
     */
    @Override
    public CacheStatsResponse getStatsByDimensions(List<CacheStatsDimension> dimensions) {
        List<CacheStatsDimension> modifiedDimensions = new ArrayList<>(dimensions);
        CacheStatsDimension tierDim = getTierDimension(dimensions);
        if (tierDim != null) {
            modifiedDimensions.remove(tierDim);
        }

        if (!checkDimensions(modifiedDimensions)) {
            throw new IllegalArgumentException("Can't retrieve stats for this combination of dimensions");
        }

        if (tierDim == null || tierDim.dimensionValue.equals(tierDimensionValue)) {
            // If there is no tier dimension, or if the tier dimension value matches the one for this stats object, return an aggregated
            // response over the non-tier dimensions
            ConcurrentMap<StatsHolder.Key, CacheStatsResponse> map = statsHolder.getStatsMap();
            CacheStatsResponse response = new CacheStatsResponse();

            // In the SEPARATE_DIMENSIONS_ONLY and SPECIFIC_COMBINATIONS cases, we don't do any adding; just return directly from the map.
            // Also do this if mode is ALL_COMBINATIONS and our dimensions have a value for every dimension name.
            if (statsHolder.mode != StatsHolder.TrackingMode.ALL_COMBINATIONS
                || modifiedDimensions.size() == statsHolder.getDimensionNames().size()) {
                CacheStatsResponse resultFromMap = map.getOrDefault(new StatsHolder.Key(modifiedDimensions), new CacheStatsResponse());
                response.add(resultFromMap); // Again return a copy
                return response;
            }

            // I don't think there's a more efficient way to get arbitrary combinations of dimensions than to just keep a map
            // and iterate through it, checking if keys match. We can't pre-aggregate because it would consume a lot of memory.
            for (StatsHolder.Key key : map.keySet()) {
                if (key.dimensions.containsAll(modifiedDimensions)) {
                    response.add(map.get(key));
                }
            }
            return response;
        }
        // If the tier dimension doesn't match, return an all-zero response
        return new CacheStatsResponse();
    }

    private CacheStatsDimension getTierDimension(List<CacheStatsDimension> dimensions) {
        for (CacheStatsDimension dim : dimensions) {
            if (dim.dimensionName.equals(CacheStatsDimension.TIER_DIMENSION_NAME)) {
                return dim;
            }
        }
        return null;
    }

    // Check the dimensions passed in are a valid request, according to the stats holder's tracking mode
    private boolean checkDimensions(List<CacheStatsDimension> dimensions) {
        switch (statsHolder.mode) {
            case SEPARATE_DIMENSIONS_ONLY:
                if (!(dimensions.size() == 1 && statsHolder.getDimensionNames().contains(dimensions.get(0).dimensionName))) {
                    return false;
                }
                break;
            case ALL_COMBINATIONS:
                for (CacheStatsDimension dim : dimensions) {
                    if (!statsHolder.getDimensionNames().contains(dim.dimensionName)) {
                        return false;
                    }
                }
                break;
            case SPECIFIC_COMBINATIONS:
                if (!statsHolder.getSpecificCombinations().contains(getDimensionNamesSet(dimensions))) {
                    return false;
                }
                break;
        }
        return true;
    }

    private Set<String> getDimensionNamesSet(List<CacheStatsDimension> dimensions) {
        Set<String> dimSet = new HashSet<>();
        for (CacheStatsDimension dim : dimensions) {
            dimSet.add(dim.dimensionName);
        }
        return dimSet;
    }

    @Override
    public long getTotalHits() {
        return statsHolder.getTotalStats().getHits();
    }

    @Override
    public long getTotalMisses() {
        return statsHolder.getTotalStats().getMisses();
    }

    @Override
    public long getTotalEvictions() {
        return statsHolder.getTotalStats().getEvictions();
    }

    @Override
    public long getTotalMemorySize() {
        return statsHolder.getTotalStats().getMemorySize();
    }

    @Override
    public long getTotalEntries() {
        return statsHolder.getTotalStats().getEntries();
    }

    @Override
    public long getHitsByDimensions(List<CacheStatsDimension> dimensions) {
        return getStatsByDimensions(dimensions).getHits();
    }

    @Override
    public long getMissesByDimensions(List<CacheStatsDimension> dimensions) {
        return getStatsByDimensions(dimensions).getMisses();
    }

    @Override
    public long getEvictionsByDimensions(List<CacheStatsDimension> dimensions) {
        return getStatsByDimensions(dimensions).getEvictions();
    }

    @Override
    public long getMemorySizeByDimensions(List<CacheStatsDimension> dimensions) {
        return getStatsByDimensions(dimensions).getMemorySize();
    }

    @Override
    public long getEntriesByDimensions(List<CacheStatsDimension> dimensions) {
        return getStatsByDimensions(dimensions).getEntries();
    }
}

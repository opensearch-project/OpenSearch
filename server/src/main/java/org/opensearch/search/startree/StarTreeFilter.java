/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@ExperimentalApi
public class StarTreeFilter {

    private final Map<String, List<DimensionFilter>> dimensionFilterMap;

    public StarTreeFilter(Map<String, List<DimensionFilter>> dimensionFilterMap) {
        this.dimensionFilterMap = dimensionFilterMap;
    }

    public List<DimensionFilter> getFiltersForDimension(String dimension) {
        return dimensionFilterMap.get(dimension);
    }

    public Set<String> getDimensions() {
        return dimensionFilterMap.keySet();
    }

    /**
     *
     * @param other
     * @return : true, if merging was successful false otherwise.
     */
    public boolean mergeStarTreeFilter(StarTreeFilter other) {
        Set<String> newDimsFromOther = new HashSet<>();
        Map<String, List<DimensionFilter>> newMergedDimensionFilterMap = new HashMap<>();
        for (String dimension : other.getDimensions()) {
            if (dimensionFilterMap.containsKey(dimension)) {
                List<DimensionFilter> mergedDimFilters = mergeDimensionFilters(dimension, other);
                if (mergedDimFilters != null) {
                    newMergedDimensionFilterMap.put(dimension, mergedDimFilters);
                } else {
                    return false; // The StarTreeFilter cannot be merged.
                }
            } else {
                newDimsFromOther.add(dimension);
            }
        }
        for (String newDim : newDimsFromOther) {
            dimensionFilterMap.put(newDim, other.getFiltersForDimension(newDim));
        }
        for (String mergedDim : newMergedDimensionFilterMap.keySet()) {
            dimensionFilterMap.put(mergedDim, newMergedDimensionFilterMap.get(mergedDim));
        }
        return true;
    }

    private List<DimensionFilter> mergeDimensionFilters(String dimension, StarTreeFilter other) {
        List<DimensionFilter> selfDimFilters = dimensionFilterMap.get(dimension);
        List<DimensionFilter> otherDimFilters = other.getFiltersForDimension(dimension);
        // TODO : Complete merging 2 dimension filters keeping the sorted logic for filters.
        // TODO : Use 2 pointer approach for merging
        return null;
    }

}

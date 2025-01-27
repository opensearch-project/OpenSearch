/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree.filter;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Container for intermediate/consolidated dimension filters that will be applied for a query in star tree traversal.
 */
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
    // TODO : Implement Merging of 2 Star Tree Filters
    // This would also involve merging 2 different types of dimension filters.
    // It also brings in the challenge of sorting input values in user query for efficient merging.
    // Merging Range with Term and Range with Range and so on.
    // All these will be implemented post OS 2.19

}

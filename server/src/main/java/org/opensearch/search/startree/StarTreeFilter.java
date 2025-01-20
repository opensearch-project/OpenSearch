/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.opensearch.common.annotation.ExperimentalApi;

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

}

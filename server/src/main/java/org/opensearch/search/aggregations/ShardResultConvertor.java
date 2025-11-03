/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.opensearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface ShardResultConvertor {

    default List<InternalAggregation> convert(Map<String, Object[]> shardResult, SearchContext searchContext) {
        int rows = shardResult.entrySet().stream().findFirst().get().getValue().length;
        List<InternalAggregation> internalAggregations = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            internalAggregations.add(convertRow(shardResult, i, searchContext));
        }
        return internalAggregations;
    }

    default InternalAggregation convertRow(Map<String, Object[]> shardResult, int row, SearchContext searchContext) {
        throw new UnsupportedOperationException("Row conversion not supported");
    }

}

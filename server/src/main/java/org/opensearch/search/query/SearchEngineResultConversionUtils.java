/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.ShardResultConvertor;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SearchEngineResultConversionUtils {

    public static InternalAggregations convertDFResultGeneric(SearchContext searchContext) {
        Map<String, Object[]> dfResult = searchContext.getDFResults();

        // Create aggregators which will process the result from DataFusion
        try {

            List<Aggregator> aggregators = new ArrayList<>();

            if (searchContext.aggregations().factories().hasGlobalAggregator()) {
                aggregators.addAll(searchContext.aggregations().factories().createTopLevelGlobalAggregators(searchContext));
            }

            if (searchContext.aggregations().factories().hasNonGlobalAggregator()) {
                aggregators.addAll(searchContext.aggregations().factories().createTopLevelNonGlobalAggregators(searchContext));
            }

            List<ShardResultConvertor> shardResultConvertors = aggregators.stream().map(x -> {
                if (x instanceof ShardResultConvertor) {
                    return ((ShardResultConvertor) x);
                } else {
                    throw new UnsupportedOperationException("Aggregator doesn't support converting results from shard: " + x);
                }
            }).toList();

            return InternalAggregations.from(
                shardResultConvertors.stream().flatMap(x -> x.convert(dfResult).stream()).collect(Collectors.toList())
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}

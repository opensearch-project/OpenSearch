/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.collect.Tuple;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.ShardResultConvertor;
import org.opensearch.search.aggregations.metrics.CardinalityAggregator;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.ValueCountAggregator;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SearchEngineResultConversionUtils {

    private static final Logger LOGGER = LogManager.getLogger(SearchEngineResultConversionUtils.class);

    public static final String INJECTED_COUNT_AGG_NAME = "agg_for_doc_count";

    public static void convertDFResultGeneric(SearchContext searchContext) {
        if (searchContext.aggregations() != null) {
            Map<String, Object[]> dfResult = searchContext.getDFResults();

//            LOGGER.info("DF Results at convertDFResultGeneric:");
//            for (Map.Entry<String, Object[]> entry : dfResult.entrySet()) {
//                LOGGER.info("{}: {}", entry.getKey(), java.util.Arrays.toString(entry.getValue()));
//            }

            // Create aggregators which will process the result from DataFusion
            try {

                List<Aggregator> aggregators = new ArrayList<>();

                if (searchContext.aggregations().factories().hasGlobalAggregator()) {
                    aggregators.addAll(searchContext.aggregations().factories().createTopLevelGlobalAggregators(searchContext.getOriginalContext()));
                }

                if (searchContext.aggregations().factories().hasNonGlobalAggregator()) {
                    aggregators.addAll(searchContext.aggregations().factories().createTopLevelNonGlobalAggregators(searchContext.getOriginalContext()));
                }

                List<ShardResultConvertor> shardResultConvertors = aggregators.stream().map(x -> {
                    if (x instanceof ShardResultConvertor) {
                        return ((ShardResultConvertor) x);
                    } else {
                        throw new UnsupportedOperationException("Aggregator doesn't support converting results from shard: " + x);
                    }
                }).toList();

                InternalAggregations internalAggregations = InternalAggregations.EMPTY;

                if(searchContext.getDFResults().isEmpty() == false) {
                    internalAggregations = InternalAggregations.from(
                        shardResultConvertors.stream().flatMap(x -> x.convert(dfResult, searchContext).stream()).collect(Collectors.toList())
                    );
                }
                //LOGGER.info("Converted DF result to internal aggregations: {}", internalAggregations.asList());
                searchContext.queryResult().aggregations(internalAggregations);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static Tuple<List<InternalAggregation>, Long> extractSubAggsAndDocCount(Aggregator[] subAggregators, SearchContext searchContext, Map<String, Object[]> shardResult, int row) {
        List<InternalAggregation> subAggs = new ArrayList<>();
        long docCount = -1;
        for (Aggregator aggregator : subAggregators) {
            if (aggregator instanceof ShardResultConvertor convertor) {
                InternalAggregation subAgg = convertor.convertRow(shardResult, row, searchContext);
                if (aggregator instanceof ValueCountAggregator) {
                    docCount = ((InternalValueCount) subAgg).getValue();
                }
//                if (aggregator instanceof CardinalityAggregator) {
//                    docCount = ((InternalCardinality) subAgg).getValue();
//                }
                subAggs.add(subAgg);
            }
        }
        if (docCount == -1) {
            Object[] values = shardResult.get(INJECTED_COUNT_AGG_NAME);
            if (values != null) {
                docCount = ((Number) values[row]).longValue();
            } else {
                throw new IllegalStateException(String.format("Unable to populate doc count from shard result [%s]", shardResult.keySet()));
            }
        }
        return new Tuple<>(subAggs, docCount);
    }

}

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
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.ShardResultConvertor;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SearchEngineResultConversionUtils {

    private static final Logger LOGGER = LogManager.getLogger(SearchEngineResultConversionUtils.class);

    public static final String INJECTED_COUNT_AGG_NAME = "agg_for_doc_count";

    public static void convertDFResultGeneric(SearchContext searchContext) {
        if (searchContext.aggregations() != null) {
            Map<String, Object[]> dfResult = searchContext.getDFResults();

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

                InternalAggregations internalAggregations = InternalAggregations.from(
                    shardResultConvertors.stream().flatMap(x -> x.convert(dfResult, searchContext).stream()).collect(Collectors.toList())
                );
                LOGGER.info("Converted DF result to internal aggregations: {}", internalAggregations.asList());
                searchContext.queryResult().aggregations(internalAggregations);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static boolean hasValueCountAggregator(Collection<AggregationBuilder> aggregationBuilders) {
        boolean hasValueCountAggregator = false;
        for (AggregationBuilder aggregationBuilder : aggregationBuilders) {
            if (aggregationBuilder instanceof ValueCountAggregationBuilder) {
                return true;
            } else {
                hasValueCountAggregator |= aggregationBuilder.getSubAggregations().isEmpty() == false && hasValueCountAggregator(aggregationBuilder.getSubAggregations());
            }
        }
        return hasValueCountAggregator;
    }

    public static String getNonMetadataFieldName(SearchContext searchContext) {
        for (MappedFieldType mappedFieldType : searchContext.mapperService().fieldTypes()) {
            if (searchContext.mapperService().isMetadataField(mappedFieldType.name()) == false) {
                return mappedFieldType.name();
            }
        }
        throw new IllegalStateException("Found no fields other than metadata in index " + searchContext.mapperService().index().getName());
    }

    public static void addValueCountIfAbsent(SearchContext searchContext, SearchSourceBuilder sourceBuilder) {
        if (sourceBuilder.aggregations() != null) {
            // There can be 2 cases :
            // <1> There is only one aggregator with sub-aggregators ( Terms, MultiTerms, DateHistogram and Composite ) ( Group By )
            // <2> There are individual metric aggregators present ( Non Group By )
            Collection<AggregationBuilder> aggregationBuilders = sourceBuilder.aggregations().getAggregatorFactories();
            String fieldName = getNonMetadataFieldName(searchContext);
            if (aggregationBuilders.size() == 1) {
                if (hasValueCountAggregator(aggregationBuilders) == false) {
                    AggregationBuilder aggregationBuilder = aggregationBuilders.stream().findFirst().get();
                    aggregationBuilder.subAggregation(new ValueCountAggregationBuilder(INJECTED_COUNT_AGG_NAME).field(fieldName));
                }
            } else {
                sourceBuilder.aggregations().addAggregator(new ValueCountAggregationBuilder(INJECTED_COUNT_AGG_NAME).field(fieldName));
            }
        }
    }

}

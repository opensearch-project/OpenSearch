/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * Aggregator factory for the boxplot aggregation.
 *
 * @opensearch.internal
 */
public class BoxplotAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final double compression;

    BoxplotAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        double compression
    ) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.compression = compression;
    }

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(BoxplotAggregationBuilder.REGISTRY_KEY, CoreValuesSourceType.NUMERIC, BoxplotAggregator::new, true);
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new BoxplotAggregator(name, null, config.format(), searchContext, parent, metadata, compression);
    }

    @Override
    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return queryShardContext.getValuesSourceRegistry()
            .getAggregator(BoxplotAggregationBuilder.REGISTRY_KEY, config)
            .build(name, config.getValuesSource(), config.format(), searchContext, parent, metadata, compression);
    }

    @Override
    protected boolean supportsConcurrentSegmentSearch() {
        return true;
    }
}

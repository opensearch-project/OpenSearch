/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.bucket.histogram;

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
 * Aggregation Factory for variable_width_histogram
 *
 * @opensearch.internal
 */
public class VariableWidthHistogramAggregatorFactory extends ValuesSourceAggregatorFactory {

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            VariableWidthHistogramAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.NUMERIC,
            VariableWidthHistogramAggregator::new,
            true
        );
    }

    private final int numBuckets;
    private final int shardSize;
    private final int initialBuffer;

    VariableWidthHistogramAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        int numBuckets,
        int shardSize,
        int initialBuffer,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.numBuckets = numBuckets;
        this.shardSize = shardSize;
        this.initialBuffer = initialBuffer;
    }

    @Override
    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        if (cardinality != CardinalityUpperBound.ONE) {
            throw new IllegalArgumentException(
                "["
                    + VariableWidthHistogramAggregationBuilder.NAME
                    + "] cannot be nested inside an aggregation that collects more than a single bucket."
            );
        }
        return queryShardContext.getValuesSourceRegistry()
            .getAggregator(VariableWidthHistogramAggregationBuilder.REGISTRY_KEY, config)
            .build(name, factories, numBuckets, shardSize, initialBuffer, config, searchContext, parent, metadata);
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new VariableWidthHistogramAggregator(
            name,
            factories,
            numBuckets,
            shardSize,
            initialBuffer,
            config,
            searchContext,
            parent,
            metadata
        );
    }

    @Override
    protected boolean supportsConcurrentSegmentSearch() {
        return true;
    }
}

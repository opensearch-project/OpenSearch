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

package org.opensearch.search.aggregations.bucket.range;

import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Aggregation Factory for binary_range
 *
 * @opensearch.internal
 */
public class BinaryRangeAggregatorFactory extends ValuesSourceAggregatorFactory {

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(IpRangeAggregationBuilder.REGISTRY_KEY, CoreValuesSourceType.IP, BinaryRangeAggregator::new, true);
    }

    private final List<BinaryRangeAggregator.Range> ranges;
    private final boolean keyed;

    public BinaryRangeAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        List<BinaryRangeAggregator.Range> ranges,
        boolean keyed,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.ranges = ranges;
        this.keyed = keyed;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new BinaryRangeAggregator(
            name,
            factories,
            null,
            config.format(),
            ranges,
            keyed,
            searchContext,
            parent,
            CardinalityUpperBound.NONE,
            metadata
        );
    }

    @Override
    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return queryShardContext.getValuesSourceRegistry()
            .getAggregator(IpRangeAggregationBuilder.REGISTRY_KEY, config)
            .build(name, factories, config.getValuesSource(), config.format(), ranges, keyed, searchContext, parent, cardinality, metadata);
    }

    @Override
    protected boolean supportsConcurrentSegmentSearch() {
        return true;
    }
}

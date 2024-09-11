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
import java.util.Locale;
import java.util.Map;

/**
 * Aggregation Factory for cardinality agg
 *
 * @opensearch.internal
 */
class CardinalityAggregatorFactory extends ValuesSourceAggregatorFactory {

    public enum ExecutionMode {

        UNSET,
        DIRECT,
        ORDINALS;

        ExecutionMode() {}

        public static ExecutionMode fromString(String value) {
            if (value == null) {
                return UNSET;
            }
            try {
                return ExecutionMode.valueOf(value.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unknown `execution_hint`: [" + value + "], expected any of [direct, ordinals]");
            }
        }

        @Override
        public String toString() {
            return this.name().toLowerCase(Locale.ROOT);
        }
    }

    private final ExecutionMode executionMode;

    private final Long precisionThreshold;

    CardinalityAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        Long precisionThreshold,
        String executionHint,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.precisionThreshold = precisionThreshold;
        this.executionMode = ExecutionMode.fromString(executionHint);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(CardinalityAggregationBuilder.REGISTRY_KEY, CoreValuesSourceType.ALL_CORE, CardinalityAggregator::new, true);
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new CardinalityAggregator(name, config, precision(), executionMode, searchContext, parent, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return queryShardContext.getValuesSourceRegistry()
            .getAggregator(CardinalityAggregationBuilder.REGISTRY_KEY, config)
            .build(name, config, precision(), executionMode, searchContext, parent, metadata);
    }

    @Override
    protected boolean supportsConcurrentSegmentSearch() {
        return true;
    }

    private int precision() {
        return precisionThreshold == null
            ? HyperLogLogPlusPlus.DEFAULT_PRECISION
            : HyperLogLogPlusPlus.precisionFromThreshold(precisionThreshold);
    }
}

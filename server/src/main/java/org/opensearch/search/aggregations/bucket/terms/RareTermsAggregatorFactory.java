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

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.core.ParseField;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.NonCollectingAggregator;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Aggregation Factory for rare_terms agg
 *
 * @opensearch.internal
 */
public class RareTermsAggregatorFactory extends ValuesSourceAggregatorFactory {
    private final IncludeExclude includeExclude;
    private final int maxDocCount;
    private final double precision;

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            RareTermsAggregationBuilder.REGISTRY_KEY,
            Arrays.asList(CoreValuesSourceType.BYTES, CoreValuesSourceType.IP),
            RareTermsAggregatorFactory.bytesSupplier(),
            true
        );

        builder.register(
            RareTermsAggregationBuilder.REGISTRY_KEY,
            Arrays.asList(CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN, CoreValuesSourceType.NUMERIC),
            RareTermsAggregatorFactory.numericSupplier(),
            true
        );
    }

    /**
     * This supplier is used for all the field types that should be aggregated as bytes/strings,
     * including those that need global ordinals
     */
    private static RareTermsAggregatorSupplier bytesSupplier() {
        return new RareTermsAggregatorSupplier() {
            @Override
            public Aggregator build(
                String name,
                AggregatorFactories factories,
                ValuesSource valuesSource,
                DocValueFormat format,
                int maxDocCount,
                double precision,
                IncludeExclude includeExclude,
                SearchContext context,
                Aggregator parent,
                CardinalityUpperBound cardinality,
                Map<String, Object> metadata,
                ValuesSourceConfig config
            ) throws IOException {

                ExecutionMode execution = ExecutionMode.MAP; // TODO global ords not implemented yet, only supports "map"

                if ((includeExclude != null) && (includeExclude.isRegexBased()) && format != DocValueFormat.RAW) {
                    throw new IllegalArgumentException(
                        "Aggregation ["
                            + name
                            + "] cannot support "
                            + "regular expression style include/exclude settings as they can only be applied to string fields. "
                            + "Use an array of values for include/exclude clauses"
                    );
                }

                return execution.create(
                    name,
                    factories,
                    valuesSource,
                    format,
                    includeExclude,
                    context,
                    parent,
                    metadata,
                    maxDocCount,
                    precision,
                    cardinality,
                    config
                );

            }
        };
    }

    /**
     * This supplier is used for all fields that expect to be aggregated as a numeric value.
     * This includes floating points, and formatted types that use numerics internally for storage (date, boolean, etc)
     */
    private static RareTermsAggregatorSupplier numericSupplier() {
        return new RareTermsAggregatorSupplier() {
            @Override
            public Aggregator build(
                String name,
                AggregatorFactories factories,
                ValuesSource valuesSource,
                DocValueFormat format,
                int maxDocCount,
                double precision,
                IncludeExclude includeExclude,
                SearchContext context,
                Aggregator parent,
                CardinalityUpperBound cardinality,
                Map<String, Object> metadata,
                ValuesSourceConfig config
            ) throws IOException {

                if ((includeExclude != null) && (includeExclude.isRegexBased())) {
                    throw new IllegalArgumentException(
                        "Aggregation ["
                            + name
                            + "] cannot support regular expression "
                            + "style include/exclude settings as they can only be applied to string fields. Use an array of numeric "
                            + "values for include/exclude clauses used to filter numeric fields"
                    );
                }

                IncludeExclude.LongFilter longFilter = null;
                if (((ValuesSource.Numeric) valuesSource).isFloatingPoint()) {
                    throw new IllegalArgumentException("RareTerms aggregation does not support floating point fields.");
                }
                if (includeExclude != null) {
                    longFilter = includeExclude.convertToLongFilter(format);
                }
                return new LongRareTermsAggregator(
                    name,
                    factories,
                    (ValuesSource.Numeric) valuesSource,
                    format,
                    context,
                    parent,
                    longFilter,
                    maxDocCount,
                    precision,
                    cardinality,
                    metadata
                );
            }
        };
    }

    RareTermsAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        IncludeExclude includeExclude,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        int maxDocCount,
        double precision
    ) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.includeExclude = includeExclude;
        this.maxDocCount = maxDocCount;
        this.precision = precision;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        final InternalAggregation aggregation = new UnmappedRareTerms(name, metadata);
        return new NonCollectingAggregator(name, searchContext, parent, factories, metadata) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return queryShardContext.getValuesSourceRegistry()
            .getAggregator(RareTermsAggregationBuilder.REGISTRY_KEY, config)
            .build(
                name,
                factories,
                config.getValuesSource(),
                config.format(),
                maxDocCount,
                precision,
                includeExclude,
                searchContext,
                parent,
                cardinality,
                metadata,
                config
            );
    }

    @Override
    protected boolean supportsConcurrentSegmentSearch() {
        return true;
    }

    /**
     * Execution mode for rare terms agg
     *
     * @opensearch.internal
     */
    public enum ExecutionMode {

        MAP(new ParseField("map")) {

            @Override
            Aggregator create(
                String name,
                AggregatorFactories factories,
                ValuesSource valuesSource,
                DocValueFormat format,
                IncludeExclude includeExclude,
                SearchContext context,
                Aggregator parent,
                Map<String, Object> metadata,
                long maxDocCount,
                double precision,
                CardinalityUpperBound cardinality,
                ValuesSourceConfig config
            ) throws IOException {
                int maxRegexLength = context.getQueryShardContext().getIndexSettings().getMaxRegexLength();
                final IncludeExclude.StringFilter filter = includeExclude == null
                    ? null
                    : includeExclude.convertToStringFilter(format, maxRegexLength);
                return new StringRareTermsAggregator(
                    name,
                    factories,
                    (ValuesSource.Bytes) valuesSource,
                    format,
                    filter,
                    context,
                    parent,
                    metadata,
                    maxDocCount,
                    precision,
                    cardinality,
                    config
                );
            }

            @Override
            boolean needsGlobalOrdinals() {
                return false;
            }

        };

        public static ExecutionMode fromString(String value, final DeprecationLogger deprecationLogger) {
            switch (value) {
                case "map":
                    return MAP;
                default:
                    throw new IllegalArgumentException("Unknown `execution_hint`: [" + value + "], expected any of [map]");
            }
        }

        private final ParseField parseField;

        ExecutionMode(ParseField parseField) {
            this.parseField = parseField;
        }

        abstract Aggregator create(
            String name,
            AggregatorFactories factories,
            ValuesSource valuesSource,
            DocValueFormat format,
            IncludeExclude includeExclude,
            SearchContext context,
            Aggregator parent,
            Map<String, Object> metadata,
            long maxDocCount,
            double precision,
            CardinalityUpperBound cardinality,
            ValuesSourceConfig config
        ) throws IOException;

        abstract boolean needsGlobalOrdinals();

        @Override
        public String toString() {
            return parseField.getPreferredName();
        }
    }

}

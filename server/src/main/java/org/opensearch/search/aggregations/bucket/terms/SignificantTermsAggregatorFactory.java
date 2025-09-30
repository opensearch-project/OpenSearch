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
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.NonCollectingAggregator;
import org.opensearch.search.aggregations.bucket.BucketUtils;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator.BucketCountThresholds;
import org.opensearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
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
 * Aggregation Factory for significant_terms agg
 *
 * @opensearch.internal
 */
public class SignificantTermsAggregatorFactory extends ValuesSourceAggregatorFactory {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(SignificantTermsAggregatorFactory.class);

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            SignificantTermsAggregationBuilder.REGISTRY_KEY,
            Arrays.asList(CoreValuesSourceType.BYTES, CoreValuesSourceType.IP),
            SignificantTermsAggregatorFactory.bytesSupplier(),
            true
        );

        builder.register(
            SignificantTermsAggregationBuilder.REGISTRY_KEY,
            Arrays.asList(CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN, CoreValuesSourceType.NUMERIC),
            SignificantTermsAggregatorFactory.numericSupplier(),
            true
        );
    }

    /**
     * This supplier is used for all the field types that should be aggregated as bytes/strings,
     * including those that need global ordinals
     */
    private static SignificantTermsAggregatorSupplier bytesSupplier() {
        return new SignificantTermsAggregatorSupplier() {
            @Override
            public Aggregator build(
                String name,
                AggregatorFactories factories,
                ValuesSource valuesSource,
                DocValueFormat format,
                TermsAggregator.BucketCountThresholds bucketCountThresholds,
                IncludeExclude includeExclude,
                String executionHint,
                SearchContext context,
                Aggregator parent,
                SignificanceHeuristic significanceHeuristic,
                SignificanceLookup lookup,
                CardinalityUpperBound cardinality,
                Map<String, Object> metadata,
                ValuesSourceConfig config
            ) throws IOException {

                ExecutionMode execution = null;
                if (executionHint != null) {
                    execution = ExecutionMode.fromString(executionHint, deprecationLogger);
                }
                if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals == false) {
                    execution = ExecutionMode.MAP;
                }
                if (execution == null) {
                    execution = ExecutionMode.GLOBAL_ORDINALS;
                }

                if ((includeExclude != null) && (includeExclude.isRegexBased()) && format != DocValueFormat.RAW) {
                    throw new IllegalArgumentException(
                        "Aggregation ["
                            + name
                            + "] cannot support regular expression style "
                            + "include/exclude settings as they can only be applied to string fields. Use an array of values for "
                            + "include/exclude clauses"
                    );
                }

                return execution.create(
                    name,
                    factories,
                    valuesSource,
                    format,
                    bucketCountThresholds,
                    includeExclude,
                    context,
                    parent,
                    significanceHeuristic,
                    lookup,
                    cardinality,
                    metadata,
                    config
                );
            }
        };
    }

    /**
     * This supplier is used for all fields that expect to be aggregated as a numeric value.
     * This includes floating points, and formatted types that use numerics internally for storage (date, boolean, etc)
     */
    private static SignificantTermsAggregatorSupplier numericSupplier() {
        return new SignificantTermsAggregatorSupplier() {
            @Override
            public Aggregator build(
                String name,
                AggregatorFactories factories,
                ValuesSource valuesSource,
                DocValueFormat format,
                TermsAggregator.BucketCountThresholds bucketCountThresholds,
                IncludeExclude includeExclude,
                String executionHint,
                SearchContext context,
                Aggregator parent,
                SignificanceHeuristic significanceHeuristic,
                SignificanceLookup lookup,
                CardinalityUpperBound cardinality,
                Map<String, Object> metadata,
                ValuesSourceConfig config
            ) throws IOException {

                if ((includeExclude != null) && (includeExclude.isRegexBased())) {
                    throw new IllegalArgumentException(
                        "Aggregation ["
                            + name
                            + "] cannot support regular expression style include/exclude "
                            + "settings as they can only be applied to string fields. Use an array of numeric "
                            + "values for include/exclude clauses used to filter numeric fields"
                    );
                }

                ValuesSource.Numeric numericValuesSource = (ValuesSource.Numeric) valuesSource;
                if (numericValuesSource.isFloatingPoint()) {
                    throw new UnsupportedOperationException("No support for examining floating point numerics");
                }

                IncludeExclude.LongFilter longFilter = null;
                if (includeExclude != null) {
                    longFilter = includeExclude.convertToLongFilter(format);
                }

                return new NumericTermsAggregator(
                    name,
                    factories,
                    agg -> agg.new SignificantLongTermsResults(lookup, significanceHeuristic, cardinality),
                    numericValuesSource,
                    format,
                    null,
                    bucketCountThresholds,
                    context,
                    parent,
                    SubAggCollectionMode.BREADTH_FIRST,
                    longFilter,
                    cardinality,
                    metadata
                );
            }
        };
    }

    private final IncludeExclude includeExclude;
    private final String executionHint;
    private final QueryBuilder backgroundFilter;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final SignificanceHeuristic significanceHeuristic;

    SignificantTermsAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        IncludeExclude includeExclude,
        String executionHint,
        QueryBuilder backgroundFilter,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        SignificanceHeuristic significanceHeuristic,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);

        if (config.hasValues()) {
            if (config.fieldContext().fieldType().isSearchable() == false) {
                throw new IllegalArgumentException(
                    "SignificantText aggregation requires fields to be searchable, but ["
                        + config.fieldContext().fieldType().name()
                        + "] is not"
                );
            }
        }

        this.includeExclude = includeExclude;
        this.executionHint = executionHint;
        this.backgroundFilter = backgroundFilter;
        this.bucketCountThresholds = bucketCountThresholds;
        this.significanceHeuristic = significanceHeuristic;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        final InternalAggregation aggregation = new UnmappedSignificantTerms(name, bucketCountThresholds, metadata);
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
        SignificantTermsAggregatorSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry()
            .getAggregator(SignificantTermsAggregationBuilder.REGISTRY_KEY, config);

        BucketCountThresholds bucketCountThresholds = new BucketCountThresholds(this.bucketCountThresholds);
        if (bucketCountThresholds.getShardSize() == SignificantTermsAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
            // The user has not made a shardSize selection .
            // Use default heuristic to avoid any wrong-ranking caused by
            // distributed counting
            // but request double the usual amount.
            // We typically need more than the number of "top" terms requested
            // by other aggregations
            // as the significance algorithm is in less of a position to
            // down-select at shard-level -
            // some of the things we want to find have only one occurrence on
            // each shard and as
            // such are impossible to differentiate from non-significant terms
            // at that early stage.
            bucketCountThresholds.setShardSize(2 * BucketUtils.suggestShardSideQueueSize(bucketCountThresholds.getRequiredSize()));
        }

        SignificanceLookup lookup = new SignificanceLookup(
            queryShardContext,
            config.fieldContext().fieldType(),
            config.format(),
            backgroundFilter
        );

        return aggregatorSupplier.build(
            name,
            factories,
            config.getValuesSource(),
            config.format(),
            bucketCountThresholds,
            includeExclude,
            executionHint,
            searchContext,
            parent,
            significanceHeuristic,
            lookup,
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
     * The execution mode for the significant terms agg
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
                TermsAggregator.BucketCountThresholds bucketCountThresholds,
                IncludeExclude includeExclude,
                SearchContext aggregationContext,
                Aggregator parent,
                SignificanceHeuristic significanceHeuristic,
                SignificanceLookup lookup,
                CardinalityUpperBound cardinality,
                Map<String, Object> metadata,
                ValuesSourceConfig config
            ) throws IOException {
                int maxRegexLength = aggregationContext.getQueryShardContext().getIndexSettings().getMaxRegexLength();
                final IncludeExclude.StringFilter filter = includeExclude == null
                    ? null
                    : includeExclude.convertToStringFilter(format, maxRegexLength);
                return new MapStringTermsAggregator(
                    name,
                    factories,
                    new MapStringTermsAggregator.ValuesSourceCollectorSource(valuesSource),
                    a -> a.new SignificantTermsResults(lookup, significanceHeuristic, cardinality),
                    null,
                    format,
                    bucketCountThresholds,
                    filter,
                    aggregationContext,
                    parent,
                    SubAggCollectionMode.BREADTH_FIRST,
                    false,
                    cardinality,
                    metadata,
                    config
                );

            }

        },
        GLOBAL_ORDINALS(new ParseField("global_ordinals")) {

            @Override
            Aggregator create(
                String name,
                AggregatorFactories factories,
                ValuesSource valuesSource,
                DocValueFormat format,
                TermsAggregator.BucketCountThresholds bucketCountThresholds,
                IncludeExclude includeExclude,
                SearchContext aggregationContext,
                Aggregator parent,
                SignificanceHeuristic significanceHeuristic,
                SignificanceLookup lookup,
                CardinalityUpperBound cardinality,
                Map<String, Object> metadata,
                ValuesSourceConfig config
            ) throws IOException {
                int maxRegexLength = aggregationContext.getQueryShardContext().getIndexSettings().getMaxRegexLength();
                final IncludeExclude.OrdinalsFilter filter = includeExclude == null
                    ? null
                    : includeExclude.convertToOrdinalsFilter(format, maxRegexLength);
                boolean remapGlobalOrd = true;
                if (cardinality == CardinalityUpperBound.ONE && factories == AggregatorFactories.EMPTY && includeExclude == null) {
                    /*
                     * We don't need to remap global ords iff this aggregator:
                     *    - collects from a single bucket AND
                     *    - has no include/exclude rules AND
                     *    - has no sub-aggregator
                     */
                    remapGlobalOrd = false;
                }

                return new GlobalOrdinalsStringTermsAggregator(
                    name,
                    factories,
                    a -> a.new SignificantTermsResults(lookup, significanceHeuristic, cardinality),
                    (ValuesSource.Bytes.WithOrdinals.FieldData) valuesSource,
                    null,
                    format,
                    bucketCountThresholds,
                    filter,
                    aggregationContext,
                    parent,
                    remapGlobalOrd,
                    SubAggCollectionMode.BREADTH_FIRST,
                    false,
                    cardinality,
                    metadata,
                    config
                );
            }
        };

        public static ExecutionMode fromString(String value, final DeprecationLogger deprecationLogger) {
            if ("global_ordinals".equals(value)) {
                return GLOBAL_ORDINALS;
            } else if ("global_ordinals_hash".equals(value)) {
                deprecationLogger.deprecate(
                    "global_ordinals_hash",
                    "global_ordinals_hash is deprecated. Please use [global_ordinals] instead."
                );
                return GLOBAL_ORDINALS;
            } else if ("map".equals(value)) {
                return MAP;
            }
            throw new IllegalArgumentException("Unknown `execution_hint`: [" + value + "], expected any of [map, global_ordinals]");
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
            TermsAggregator.BucketCountThresholds bucketCountThresholds,
            IncludeExclude includeExclude,
            SearchContext aggregationContext,
            Aggregator parent,
            SignificanceHeuristic significanceHeuristic,
            SignificanceLookup lookup,
            CardinalityUpperBound cardinality,
            Map<String, Object> metadata,
            ValuesSourceConfig config
        ) throws IOException;

        @Override
        public String toString() {
            return parseField.getPreferredName();
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.common.collect.Tuple;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.BucketUtils;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder.REGISTRY_KEY;

/**
 * Factory of {@link MultiTermsAggregator}.
 *
 * @opensearch.internal
 */
public class MultiTermsAggregationFactory extends AggregatorFactory {

    private final List<Tuple<ValuesSourceConfig, IncludeExclude>> configs;
    private final List<DocValueFormat> formats;
    /**
     * Fields inherent from Terms Aggregation Factory.
     */
    private final BucketOrder order;
    private final Aggregator.SubAggCollectionMode collectMode;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final boolean showTermDocCountError;

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(REGISTRY_KEY, List.of(CoreValuesSourceType.BYTES, CoreValuesSourceType.IP), config -> {
            final IncludeExclude.StringFilter filter = config.v2() == null ? null : config.v2().convertToStringFilter(config.v1().format());
            return MultiTermsAggregator.InternalValuesSourceFactory.bytesValuesSource(config.v1().getValuesSource(), filter);
        }, true);

        builder.register(
            REGISTRY_KEY,
            List.of(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.BOOLEAN, CoreValuesSourceType.DATE),
            config -> {
                ValuesSourceConfig valuesSourceConfig = config.v1();
                IncludeExclude includeExclude = config.v2();
                ValuesSource.Numeric valuesSource = ((ValuesSource.Numeric) valuesSourceConfig.getValuesSource());
                IncludeExclude.LongFilter longFilter = null;
                if (valuesSource.isFloatingPoint()) {
                    if (includeExclude != null) {
                        longFilter = includeExclude.convertToDoubleFilter();
                    }
                    return MultiTermsAggregator.InternalValuesSourceFactory.doubleValueSource(valuesSource, longFilter);
                } else if (valuesSource.isBigInteger()) {
                    if (includeExclude != null) {
                        longFilter = includeExclude.convertToDoubleFilter();
                    }
                    return MultiTermsAggregator.InternalValuesSourceFactory.unsignedLongValuesSource(valuesSource, longFilter);
                } else {
                    if (includeExclude != null) {
                        longFilter = includeExclude.convertToLongFilter(valuesSourceConfig.format());
                    }
                    return MultiTermsAggregator.InternalValuesSourceFactory.longValuesSource(valuesSource, longFilter);
                }
            },
            true
        );

        builder.registerUsage(MultiTermsAggregationBuilder.NAME);
    }

    public MultiTermsAggregationFactory(
        String name,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        List<MultiTermsValuesSourceConfig> multiTermConfigs,
        BucketOrder order,
        Aggregator.SubAggCollectionMode collectMode,
        TermsAggregator.BucketCountThresholds bucketCountThresholds,
        boolean showTermDocCountError
    ) throws IOException {
        super(name, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.configs = multiTermConfigs.stream()
            .map(
                c -> new Tuple<ValuesSourceConfig, IncludeExclude>(
                    ValuesSourceConfig.resolveUnregistered(
                        queryShardContext,
                        c.getUserValueTypeHint(),
                        c.getFieldName(),
                        c.getScript(),
                        c.getMissing(),
                        c.getTimeZone(),
                        c.getFormat(),
                        CoreValuesSourceType.BYTES
                    ),
                    c.getIncludeExclude()
                )
            )
            .collect(Collectors.toList());
        this.formats = this.configs.stream().map(c -> c.v1().format()).collect(Collectors.toList());
        this.order = order;
        this.collectMode = collectMode;
        this.bucketCountThresholds = bucketCountThresholds;
        this.showTermDocCountError = showTermDocCountError;
    }

    @Override
    protected Aggregator createInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        TermsAggregator.BucketCountThresholds bucketCountThresholds = new TermsAggregator.BucketCountThresholds(this.bucketCountThresholds);
        if (InternalOrder.isKeyOrder(order) == false
            && bucketCountThresholds.getShardSize() == TermsAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
            // The user has not made a shardSize selection. Use default
            // heuristic to avoid any wrong-ranking caused by distributed
            // counting
            bucketCountThresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(bucketCountThresholds.getRequiredSize()));
        }
        bucketCountThresholds.ensureValidity();
        return new MultiTermsAggregator(
            name,
            factories,
            showTermDocCountError,
            configs.stream()
                .map(config -> queryShardContext.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config.v1()).build(config))
                .collect(Collectors.toList()),
            configs.stream().map(c -> c.v1().format()).collect(Collectors.toList()),
            order,
            collectMode,
            bucketCountThresholds,
            searchContext,
            parent,
            cardinality,
            metadata
        );
    }

    /**
     * Supplier for internal values source
     *
     * @opensearch.internal
     */
    public interface InternalValuesSourceSupplier {
        MultiTermsAggregator.InternalValuesSource build(Tuple<ValuesSourceConfig, IncludeExclude> config);
    }
}

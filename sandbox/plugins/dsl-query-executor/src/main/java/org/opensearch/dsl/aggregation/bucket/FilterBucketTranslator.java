/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.AggregationTranslator;
import org.opensearch.dsl.aggregation.FieldGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.InternalFilter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Translates a {@link FilterAggregationBuilder} — single-bucket aggregation scoped by a query predicate.
 */
public class FilterBucketTranslator implements BucketTranslator<FilterAggregationBuilder> {

    @Override
    public Class<FilterAggregationBuilder> getAggregationType() {
        return FilterAggregationBuilder.class;
    }

    @Override
    public GroupingInfo getGrouping(FilterAggregationBuilder agg) {
        return new FieldGrouping(List.of());
    }

    @Override
    public Collection<AggregationBuilder> getSubAggregations(FilterAggregationBuilder agg) {
        return agg.getSubAggregations();
    }

    @Override
    public BucketOrder getBucketOrder(FilterAggregationBuilder agg) {
        return null;
    }

    @Override
    public Optional<QueryBuilder> getFilterQuery(FilterAggregationBuilder agg) {
        return Optional.of(agg.getFilter());
    }

    @Override
    public void validate(FilterAggregationBuilder agg) throws ConversionException {
        // No unsupported parameters. Inner query translatability is checked at plan-build time.
    }

    @Override
    public InternalAggregation toBucketAggregation(FilterAggregationBuilder agg, Iterable<BucketEntry> buckets) {
        List<BucketEntry> entries = new ArrayList<>();
        buckets.forEach(entries::add);
        if (entries.isEmpty()) {
            return new InternalFilter(agg.getName(), 0, InternalAggregations.EMPTY, AggregationTranslator.userMetadata(agg));
        }
        BucketEntry entry = entries.get(0);
        return new InternalFilter(agg.getName(), entry.docCount(), entry.subAggs(), AggregationTranslator.userMetadata(agg));
    }
}

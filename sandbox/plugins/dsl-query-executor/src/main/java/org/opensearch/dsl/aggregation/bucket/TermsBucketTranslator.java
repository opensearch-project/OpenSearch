/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.apache.lucene.util.BytesRef;
import org.opensearch.dsl.aggregation.FieldGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Translates a {@link TermsAggregationBuilder} — single-field GROUP BY.
 * {@code {"aggs": {"by_brand": {"terms": {"field": "brand"}}}}} becomes {@code GROUP BY brand}.
 */
public class TermsBucketTranslator implements BucketTranslator<TermsAggregationBuilder> {

    /** Creates a terms bucket translator. */
    public TermsBucketTranslator() {}

    @Override
    public Class<TermsAggregationBuilder> getAggregationType() {
        return TermsAggregationBuilder.class;
    }

    @Override
    public GroupingInfo getGrouping(TermsAggregationBuilder agg) {
        return new FieldGrouping(List.of(agg.field()));
    }

    @Override
    public Collection<AggregationBuilder> getSubAggregations(TermsAggregationBuilder agg) {
        return agg.getSubAggregations();
    }

    @Override
    public BucketOrder getBucketOrder(TermsAggregationBuilder agg) {
        return agg.order();
    }

    /**
     * Builds a {@link StringTerms} (keys rendered via {@code toString}; per-key-type
     * Long/Double variants are follow-up work). Shard accounting fields are zero — the
     * analytics path computes exact groups with no per-shard truncation. Bucket order is
     * already established by the plan's post-aggregate sort.
     */
    @Override
    public InternalAggregation toBucketAggregation(TermsAggregationBuilder agg, Iterable<BucketEntry> buckets) {
        List<StringTerms.Bucket> termBuckets = new ArrayList<>();
        for (BucketEntry entry : buckets) {
            Object key = entry.keys().get(0);
            if (key == null) {
                // SQL GROUP BY emits a NULL group; legacy terms excludes docs with a
                // missing field entirely (no bucket) unless "missing" is configured.
                continue;
            }
            BytesRef term = new BytesRef(key.toString());
            termBuckets.add(new StringTerms.Bucket(term, entry.docCount(), entry.subAggs(), false, 0, DocValueFormat.RAW));
        }
        BucketOrder order = agg.order();
        TermsAggregator.BucketCountThresholds thresholds = new TermsAggregator.BucketCountThresholds(
            agg.minDocCount(),
            agg.shardMinDocCount(),
            agg.size(),
            agg.shardSize()
        );
        return new StringTerms(
            agg.getName(),
            order,
            order,
            null,
            DocValueFormat.RAW,
            agg.shardSize(),
            false,
            0,
            termBuckets,
            0,
            thresholds
        );
    }
}

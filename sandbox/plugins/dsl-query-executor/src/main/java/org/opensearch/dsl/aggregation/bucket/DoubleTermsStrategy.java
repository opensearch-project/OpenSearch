/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.AggregationTranslator;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds a {@link DoubleTerms} response for floating-point key types (float, double,
 * half_float, scaled_float). Constructor argument semantics match {@link LongTermsStrategy}.
 */
public final class DoubleTermsStrategy implements TermsResponseStrategy {

    /** Singleton instance. */
    public static final DoubleTermsStrategy INSTANCE = new DoubleTermsStrategy();

    private DoubleTermsStrategy() {}

    @Override
    public InternalAggregation build(TermsAggregationBuilder agg, List<BucketEntry> entries, long otherDocCount, DocValueFormat format) {
        List<DoubleTerms.Bucket> termBuckets = new ArrayList<>(entries.size());
        for (BucketEntry entry : entries) {
            double term = ((Number) entry.keys().get(0)).doubleValue();
            termBuckets.add(new DoubleTerms.Bucket(term, entry.docCount(), entry.subAggs(), false, 0, format));
        }
        BucketOrder order = agg.order();
        return new DoubleTerms(
            agg.getName(),
            order,
            order,
            AggregationTranslator.userMetadata(agg),
            format,
            agg.shardSize(),
            false,
            otherDocCount,
            termBuckets,
            0,
            TermsBucketTranslator.thresholds(agg)
        );
    }
}

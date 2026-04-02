/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.DateHistogramGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Translates date_histogram bucket aggregation.
 */
public class DateHistogramBucketTranslator implements BucketTranslator<DateHistogramAggregationBuilder> {

    @Override
    public Class<DateHistogramAggregationBuilder> getAggregationType() {
        return DateHistogramAggregationBuilder.class;
    }

    @Override
    public GroupingInfo getGrouping(DateHistogramAggregationBuilder agg) {
        return new DateHistogramGrouping(agg.getName(), agg.field(), agg.getCalendarInterval(), agg.getFixedInterval());
    }

    @Override
    public BucketOrder getOrder(DateHistogramAggregationBuilder agg) {
        return agg.order();
    }

    @Override
    public Collection<AggregationBuilder> getSubAggregations(DateHistogramAggregationBuilder agg) {
        return agg.getSubAggregations();
    }

    @Override
    public InternalAggregation toBucketAggregation(DateHistogramAggregationBuilder agg, List<BucketEntry> buckets) {
        List<InternalDateHistogram.Bucket> dateHistogramBuckets;

        if (agg.minDocCount() == 0 && buckets.size() > 1) {
            dateHistogramBuckets = fillEmptyBuckets(buckets, agg);
        } else {
            dateHistogramBuckets = convertBuckets(buckets, agg);
        }

        long minDocCount = agg.minDocCount() == 0 ? 1 : agg.minDocCount();

        return new InternalDateHistogram(
            agg.getName(), dateHistogramBuckets, agg.order(), minDocCount,
            0L, null, DocValueFormat.RAW, agg.keyed(), Map.of()
        );
    }

    private List<InternalDateHistogram.Bucket> convertBuckets(List<BucketEntry> buckets, DateHistogramAggregationBuilder agg) {
        List<InternalDateHistogram.Bucket> result = new ArrayList<>(buckets.size());
        for (BucketEntry entry : buckets) {
            long key = ((Number) entry.keys().get(0)).longValue();
            result.add(new InternalDateHistogram.Bucket(
                key, entry.docCount(), agg.keyed(), DocValueFormat.RAW, entry.subAggs()
            ));
        }
        return result;
    }

    private List<InternalDateHistogram.Bucket> fillEmptyBuckets(List<BucketEntry> buckets, DateHistogramAggregationBuilder agg) {
        long intervalMillis = getIntervalMillis(agg);
        long minKey = ((Number) buckets.get(0).keys().get(0)).longValue();
        long maxKey = ((Number) buckets.get(buckets.size() - 1).keys().get(0)).longValue();

        Map<Long, BucketEntry> bucketMap = new HashMap<>();
        for (BucketEntry entry : buckets) {
            long key = ((Number) entry.keys().get(0)).longValue();
            bucketMap.put(key, entry);
        }

        List<InternalDateHistogram.Bucket> result = new ArrayList<>();
        for (long key = minKey; key <= maxKey; key += intervalMillis) {
            BucketEntry entry = bucketMap.get(key);
            if (entry != null) {
                result.add(new InternalDateHistogram.Bucket(
                    key, entry.docCount(), agg.keyed(), DocValueFormat.RAW, entry.subAggs()
                ));
            } else {
                result.add(new InternalDateHistogram.Bucket(
                    key, 0, agg.keyed(), DocValueFormat.RAW, InternalAggregations.EMPTY
                ));
            }
        }
        return result;
    }

    private long getIntervalMillis(DateHistogramAggregationBuilder agg) {
        if (agg.getFixedInterval() != null) {
            return agg.getFixedInterval().estimateMillis();
        } else if (agg.getCalendarInterval() != null) {
            return agg.getCalendarInterval().estimateMillis();
        }
        throw new IllegalArgumentException("No interval specified for date_histogram");
    }
}

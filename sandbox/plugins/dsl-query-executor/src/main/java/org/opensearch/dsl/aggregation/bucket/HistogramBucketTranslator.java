/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.aggregation.HistogramGrouping;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Translates histogram bucket aggregation.
 */
public class HistogramBucketTranslator implements BucketTranslator<HistogramAggregationBuilder> {

    @Override
    public Class<HistogramAggregationBuilder> getAggregationType() {
        return HistogramAggregationBuilder.class;
    }

    @Override
    public GroupingInfo getGrouping(HistogramAggregationBuilder agg) {
        return new HistogramGrouping(agg.getName(), agg.field(), agg.interval(), 0.0);
    }

    @Override
    public BucketOrder getOrder(HistogramAggregationBuilder agg) {
        return agg.order();
    }

    @Override
    public Collection<AggregationBuilder> getSubAggregations(HistogramAggregationBuilder agg) {
        return agg.getSubAggregations();
    }

    @Override
    public InternalAggregation toBucketAggregation(HistogramAggregationBuilder agg, List<BucketEntry> buckets) {
        List<InternalHistogram.Bucket> histogramBuckets;

        if (agg.minDocCount() == 0 && buckets.size() > 1) {
            histogramBuckets = fillEmptyBuckets(buckets, agg);
        } else {
            histogramBuckets = convertBuckets(buckets, agg);
        }

        long minDocCount = agg.minDocCount() == 0 ? 1 : agg.minDocCount();

        return new InternalHistogram(
            agg.getName(), histogramBuckets, agg.order(), minDocCount,
            null, DocValueFormat.RAW, agg.keyed(), Map.of()
        );
    }

    private List<InternalHistogram.Bucket> convertBuckets(List<BucketEntry> buckets, HistogramAggregationBuilder agg) {
        List<InternalHistogram.Bucket> result = new ArrayList<>(buckets.size());
        for (BucketEntry entry : buckets) {
            double key = ((Number) entry.keys().get(0)).doubleValue();
            result.add(new InternalHistogram.Bucket(
                key, entry.docCount(), agg.keyed(), DocValueFormat.RAW, entry.subAggs()
            ));
        }
        return result;
    }

    private List<InternalHistogram.Bucket> fillEmptyBuckets(List<BucketEntry> buckets, HistogramAggregationBuilder agg) {
        double interval = agg.interval();
        double minKey = ((Number) buckets.get(0).keys().get(0)).doubleValue();
        double maxKey = ((Number) buckets.get(buckets.size() - 1).keys().get(0)).doubleValue();

        Map<Double, BucketEntry> bucketMap = new HashMap<>();
        for (BucketEntry entry : buckets) {
            double key = ((Number) entry.keys().get(0)).doubleValue();
            bucketMap.put(key, entry);
        }

        List<InternalHistogram.Bucket> result = new ArrayList<>();
        for (double key = minKey; key <= maxKey + 0.0001; key += interval) {
            BucketEntry entry = bucketMap.get(key);
            if (entry != null) {
                result.add(new InternalHistogram.Bucket(
                    key, entry.docCount(), agg.keyed(), DocValueFormat.RAW, entry.subAggs()
                ));
            } else {
                result.add(new InternalHistogram.Bucket(
                    key, 0, agg.keyed(), DocValueFormat.RAW, InternalAggregations.EMPTY
                ));
            }
        }
        return result;
    }
}

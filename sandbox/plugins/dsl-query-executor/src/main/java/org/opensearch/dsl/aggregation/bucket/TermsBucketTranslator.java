/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.dsl.aggregation.AggregationTranslator;
import org.opensearch.dsl.aggregation.FieldGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Base64;
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
     * Builds the terms response with classic-path key typing, sampled from the first bucket key:
     * integral keys → {@link LongTerms}, floating → {@link DoubleTerms}, booleans → {@link LongTerms}
     * with the BOOLEAN format, binary (ip) keys render as address strings, else {@link StringTerms}.
     * Buckets are filtered by {@code min_doc_count}, sorted by the requested order, and truncated to
     * {@code size}; truncated bucket counts are reported as {@code sum_other_doc_count}.
     */
    @Override
    public InternalAggregation toBucketAggregation(TermsAggregationBuilder agg, Iterable<BucketEntry> buckets) {
        List<BucketEntry> kept = new ArrayList<>();
        for (BucketEntry entry : buckets) {
            if (entry.keys().get(0) == null) {
                // SQL GROUP BY emits a NULL group; legacy terms excludes docs with a
                // missing field entirely (no bucket) unless "missing" is configured.
                continue;
            }
            if (entry.docCount() < agg.minDocCount()) {
                continue;
            }
            kept.add(entry);
        }

        Object sample = kept.isEmpty() ? null : kept.get(0).keys().get(0);
        if (sample instanceof Boolean) {
            return longTerms(agg, kept, DocValueFormat.BOOLEAN);
        }
        if (sample instanceof Double || sample instanceof Float) {
            return doubleTerms(agg, kept);
        }
        if (sample instanceof Number) {
            return longTerms(agg, kept, DocValueFormat.RAW);
        }
        return stringTerms(agg, kept);
    }

    /** Builds a {@link StringTerms}; string and binary (ip) keys land here. */
    private static InternalAggregation stringTerms(TermsAggregationBuilder agg, List<BucketEntry> entries) {
        List<StringTerms.Bucket> termBuckets = new ArrayList<>(entries.size());
        for (BucketEntry entry : entries) {
            BytesRef term = new BytesRef(keyString(entry.keys().get(0)));
            termBuckets.add(new StringTerms.Bucket(term, entry.docCount(), entry.subAggs(), false, 0, DocValueFormat.RAW));
        }
        Truncated<StringTerms.Bucket> visible = sortAndTruncate(termBuckets, agg);
        BucketOrder order = agg.order();
        return new StringTerms(
            agg.getName(),
            order, // reduceOrder: the bucket list is already sorted by it (see sortAndTruncate)
            order, // the user-requested display order
            AggregationTranslator.userMetadata(agg),
            DocValueFormat.RAW, // keyword parity: the mapping-resolved format for string keys is RAW
            agg.shardSize(), // request echo — no shard fan-out on this path
            false, // no per-bucket doc count error rendering
            visible.otherDocCount(),
            visible.buckets(),
            0, // exact single-plan path: doc_count_error_upper_bound is truly 0
            thresholds(agg)
        );
    }

    /**
     * Builds a {@link LongTerms} for integral keys; booleans ride along as 0/1 with the BOOLEAN
     * format. Constructor argument semantics match {@link #stringTerms}.
     */
    private static InternalAggregation longTerms(TermsAggregationBuilder agg, List<BucketEntry> entries, DocValueFormat format) {
        List<LongTerms.Bucket> termBuckets = new ArrayList<>(entries.size());
        for (BucketEntry entry : entries) {
            Object key = entry.keys().get(0);
            long term = key instanceof Boolean bool ? (bool ? 1L : 0L) : ((Number) key).longValue();
            termBuckets.add(new LongTerms.Bucket(term, entry.docCount(), entry.subAggs(), false, 0, format));
        }
        Truncated<LongTerms.Bucket> visible = sortAndTruncate(termBuckets, agg);
        BucketOrder order = agg.order();
        return new LongTerms(
            agg.getName(),
            order,
            order,
            AggregationTranslator.userMetadata(agg),
            format,
            agg.shardSize(),
            false,
            visible.otherDocCount(),
            visible.buckets(),
            0,
            thresholds(agg)
        );
    }

    /**
     * Builds a {@link DoubleTerms} for floating-point keys. Constructor argument semantics match
     * {@link #stringTerms}.
     */
    private static InternalAggregation doubleTerms(TermsAggregationBuilder agg, List<BucketEntry> entries) {
        List<DoubleTerms.Bucket> termBuckets = new ArrayList<>(entries.size());
        for (BucketEntry entry : entries) {
            double term = ((Number) entry.keys().get(0)).doubleValue();
            termBuckets.add(new DoubleTerms.Bucket(term, entry.docCount(), entry.subAggs(), false, 0, DocValueFormat.RAW));
        }
        Truncated<DoubleTerms.Bucket> visible = sortAndTruncate(termBuckets, agg);
        BucketOrder order = agg.order();
        return new DoubleTerms(
            agg.getName(),
            order,
            order,
            AggregationTranslator.userMetadata(agg),
            DocValueFormat.RAW,
            agg.shardSize(),
            false,
            visible.otherDocCount(),
            visible.buckets(),
            0,
            thresholds(agg)
        );
    }

    /** Result of {@link #sortAndTruncate}: the visible buckets and the truncated tail's doc count. */
    private record Truncated<B extends MultiBucketsAggregation.Bucket>(List<B> buckets, long otherDocCount) {
    }

    /**
     * Sorts buckets per this aggregation's own order and truncates to {@code size}. The re-sort is
     * required because sibling aggregations sharing a granularity share one plan-level sort, which
     * cannot satisfy two different requested orders.
     */
    private static <B extends MultiBucketsAggregation.Bucket> Truncated<B> sortAndTruncate(
        List<B> termBuckets,
        TermsAggregationBuilder agg
    ) {
        termBuckets.sort(agg.order().comparator());
        long otherDocCount = 0;
        List<B> visible = termBuckets;
        if (termBuckets.size() > agg.size()) {
            for (int i = agg.size(); i < termBuckets.size(); i++) {
                otherDocCount += termBuckets.get(i).getDocCount();
            }
            // Copy rather than clear the tail in place: releases the full-size backing array.
            visible = new ArrayList<>(termBuckets.subList(0, agg.size()));
        }
        return new Truncated<>(visible, otherDocCount);
    }

    /** Binary keys are ip columns: render the address string like classic ip terms. */
    private static String keyString(Object key) {
        if (key instanceof byte[] bytes) {
            try {
                return NetworkAddress.format(InetAddress.getByAddress(bytes));
            } catch (UnknownHostException e) {
                // Not a 4/16-byte address; fall back to a printable, deterministic form.
                return Base64.getEncoder().encodeToString(bytes);
            }
        }
        return key.toString();
    }

    /** Bundles the request's bucket-count knobs for the result constructors. */
    private static TermsAggregator.BucketCountThresholds thresholds(TermsAggregationBuilder agg) {
        return new TermsAggregator.BucketCountThresholds(agg.minDocCount(), agg.shardMinDocCount(), agg.size(), agg.shardSize());
    }
}

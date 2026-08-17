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
import org.opensearch.dsl.converter.ConversionException;
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
import java.util.Map;

/**
 * Translates a {@link TermsAggregationBuilder} — single-field GROUP BY.
 * {@code {"aggs": {"by_brand": {"terms": {"field": "brand"}}}}} becomes {@code GROUP BY brand}.
 */
public class TermsBucketTranslator implements SizedBucketTranslator<TermsAggregationBuilder> {

    /** Creates a terms bucket translator. */
    public TermsBucketTranslator() {}

    @Override
    public Class<TermsAggregationBuilder> getAggregationType() {
        return TermsAggregationBuilder.class;
    }

    @Override
    public GroupingInfo getGrouping(TermsAggregationBuilder agg) {
        return agg.missing() == null
            ? new FieldGrouping(List.of(agg.field()))
            : new FieldGrouping(List.of(agg.field()), Map.of(agg.field(), agg.missing()));
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
     * Rejects {@code include}/{@code exclude}, {@code script}, and {@code min_doc_count: 0}:
     * the translation implements none of them, and each would change the bucket set relative
     * to classic search if ignored.
     */
    @Override
    public void validate(TermsAggregationBuilder agg) throws ConversionException {
        if (agg.includeExclude() != null) {
            throw new ConversionException(
                "[include]/[exclude] on terms aggregation [" + agg.getName() + "] is not supported by the DSL execution path"
            );
        }
        if (agg.script() != null) {
            throw new ConversionException(
                "[script] on terms aggregation [" + agg.getName() + "] is not supported by the DSL execution path"
            );
        }
        if (agg.minDocCount() == 0) {
            throw new ConversionException(
                "[min_doc_count: 0] on terms aggregation ["
                    + agg.getName()
                    + "] is not supported by the DSL execution path — zero-count buckets require enumerating the index term "
                    + "dictionary, which a GROUP BY over matching documents cannot produce"
            );
        }
    }

    @Override
    public int size(TermsAggregationBuilder agg) {
        return agg.size();
    }

    @Override
    public long minDocCount(TermsAggregationBuilder agg) {
        return agg.minDocCount();
    }

    /**
     * Renders the empty aggregation for levels with no result rows — the only case that
     * reaches this method: every terms plan is bounded (root levels by a flat LIMIT, nested
     * levels by the per-parent ROW_NUMBER window), so non-empty levels render through
     * {@link #toBucketAggregation(TermsAggregationBuilder, Iterable, long)}. A non-empty
     * bucket set here means the sized dispatch was bypassed, and the method throws.
     */
    @Override
    public InternalAggregation toBucketAggregation(TermsAggregationBuilder agg, Iterable<BucketEntry> buckets) {
        List<BucketEntry> entries = toList(buckets);
        if (entries.isEmpty() == false) {
            throw new IllegalStateException(
                "terms aggregation ["
                    + agg.getName()
                    + "] received buckets on the unsized render path: every terms plan is bounded, so rendering must go through the sized path"
            );
        }
        return render(agg, entries, 0L);
    }

    /**
     * Renders the plan's bucket set as received: the plan already excluded null keys, applied
     * {@code min_doc_count}, ordered, and truncated to the top {@code size}.
     * {@code sum_other_doc_count} is {@code eligibleDocCount − Σ(received doc counts)}.
     */
    @Override
    public InternalAggregation toBucketAggregation(TermsAggregationBuilder agg, Iterable<BucketEntry> buckets, long eligibleDocCount) {
        return render(agg, toList(buckets), eligibleDocCount);
    }

    private static List<BucketEntry> toList(Iterable<BucketEntry> buckets) {
        List<BucketEntry> entries = new ArrayList<>();
        buckets.forEach(entries::add);
        return entries;
    }

    /**
     * Builds the terms response with classic-path key typing, sampled from the first bucket key:
     * integral keys → {@link LongTerms}, floating → {@link DoubleTerms}, booleans → {@link LongTerms}
     * with the BOOLEAN format, binary (ip) keys render as address strings, else {@link StringTerms}.
     * {@code eligibleDocCount} supplies the total {@code sum_other_doc_count} is subtracted from (see
     * {@link #sumOtherDocCount}).
     */
    private static InternalAggregation render(TermsAggregationBuilder agg, List<BucketEntry> kept, long eligibleDocCount) {
        Object sample = kept.isEmpty() ? null : kept.get(0).keys().get(0);
        if (sample instanceof Boolean) {
            return longTerms(agg, kept, DocValueFormat.BOOLEAN, eligibleDocCount);
        }
        if (sample instanceof Double || sample instanceof Float) {
            return doubleTerms(agg, kept, eligibleDocCount);
        }
        if (sample instanceof Number) {
            return longTerms(agg, kept, DocValueFormat.RAW, eligibleDocCount);
        }
        return stringTerms(agg, kept, eligibleDocCount);
    }

    /** Builds a {@link StringTerms}; string and binary (ip) keys land here. */
    private static InternalAggregation stringTerms(TermsAggregationBuilder agg, List<BucketEntry> entries, long eligibleDocCount) {
        List<StringTerms.Bucket> termBuckets = new ArrayList<>(entries.size());
        for (BucketEntry entry : entries) {
            BytesRef term = new BytesRef(keyString(entry.keys().get(0)));
            termBuckets.add(new StringTerms.Bucket(term, entry.docCount(), entry.subAggs(), false, 0, DocValueFormat.RAW));
        }
        BucketOrder order = agg.order();
        return new StringTerms(
            agg.getName(),
            order, // reduceOrder: the plan sorted the bucket list by it
            order, // the user-requested display order
            AggregationTranslator.userMetadata(agg),
            DocValueFormat.RAW, // keyword parity: the mapping-resolved format for string keys is RAW
            agg.shardSize(), // request echo — no shard fan-out on this path
            false, // no per-bucket doc count error rendering
            sumOtherDocCount(termBuckets, eligibleDocCount),
            termBuckets,
            0, // exact single-plan path: doc_count_error_upper_bound is truly 0
            thresholds(agg)
        );
    }

    /**
     * Builds a {@link LongTerms} for integral keys; booleans ride along as 0/1 with the BOOLEAN
     * format. Constructor argument semantics match {@link #stringTerms}.
     */
    private static InternalAggregation longTerms(
        TermsAggregationBuilder agg,
        List<BucketEntry> entries,
        DocValueFormat format,
        long eligibleDocCount
    ) {
        List<LongTerms.Bucket> termBuckets = new ArrayList<>(entries.size());
        for (BucketEntry entry : entries) {
            Object key = entry.keys().get(0);
            long term = key instanceof Boolean bool ? (bool ? 1L : 0L) : ((Number) key).longValue();
            termBuckets.add(new LongTerms.Bucket(term, entry.docCount(), entry.subAggs(), false, 0, format));
        }
        BucketOrder order = agg.order();
        return new LongTerms(
            agg.getName(),
            order,
            order,
            AggregationTranslator.userMetadata(agg),
            format,
            agg.shardSize(),
            false,
            sumOtherDocCount(termBuckets, eligibleDocCount),
            termBuckets,
            0,
            thresholds(agg)
        );
    }

    /**
     * Builds a {@link DoubleTerms} for floating-point keys. Constructor argument semantics match
     * {@link #stringTerms}.
     */
    private static InternalAggregation doubleTerms(TermsAggregationBuilder agg, List<BucketEntry> entries, long eligibleDocCount) {
        List<DoubleTerms.Bucket> termBuckets = new ArrayList<>(entries.size());
        for (BucketEntry entry : entries) {
            double term = ((Number) entry.keys().get(0)).doubleValue();
            termBuckets.add(new DoubleTerms.Bucket(term, entry.docCount(), entry.subAggs(), false, 0, DocValueFormat.RAW));
        }
        BucketOrder order = agg.order();
        return new DoubleTerms(
            agg.getName(),
            order,
            order,
            AggregationTranslator.userMetadata(agg),
            DocValueFormat.RAW,
            agg.shardSize(),
            false,
            sumOtherDocCount(termBuckets, eligibleDocCount),
            termBuckets,
            0,
            thresholds(agg)
        );
    }

    /**
     * Computes {@code sum_other_doc_count} — docs belonging to groups not in the bucket
     * list — as {@code eligibleDocCount − Σ(received doc counts)}, clamped at zero: root
     * eligible counts come from a separately executed COUNT plan, and a refresh landing between
     * the two queries can leave the eligible count smaller than the received sum. (Nested
     * eligible counts ride the plan's own rows and cannot skew.)
     */
    private static long sumOtherDocCount(List<? extends MultiBucketsAggregation.Bucket> termBuckets, long eligibleDocCount) {
        long receivedDocCount = 0;
        for (MultiBucketsAggregation.Bucket bucket : termBuckets) {
            receivedDocCount += bucket.getDocCount();
        }
        return Math.max(0, eligibleDocCount - receivedDocCount);
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

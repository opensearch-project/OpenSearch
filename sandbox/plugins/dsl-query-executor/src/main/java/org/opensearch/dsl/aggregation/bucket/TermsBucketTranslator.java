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
import org.opensearch.dsl.aggregation.FieldGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
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
     * Builds the terms response with classic-path key typing: integral keys → {@link LongTerms},
     * floating → {@link DoubleTerms}, booleans → {@link LongTerms} with the BOOLEAN format,
     * binary (ip) keys decode to address strings, else {@link StringTerms}. Shard accounting
     * fields are zero — the analytics path computes exact groups with no per-shard truncation.
     */
    @Override
    public InternalAggregation toBucketAggregation(TermsAggregationBuilder agg, Iterable<BucketEntry> buckets) {
        // SQL GROUP BY emits a NULL group; legacy terms excludes docs with a missing
        // field entirely (no bucket) unless "missing" is configured.
        List<BucketEntry> nonNull = new ArrayList<>();
        for (BucketEntry entry : buckets) {
            if (entry.keys().get(0) != null) {
                nonNull.add(entry);
            }
        }
        Object sample = nonNull.isEmpty() ? null : nonNull.get(0).keys().get(0);
        if (sample instanceof Boolean) {
            return longTerms(agg, nonNull, DocValueFormat.BOOLEAN);
        }
        if (sample instanceof Double || sample instanceof Float) {
            return doubleTerms(agg, nonNull);
        }
        if (sample instanceof Number) {
            return longTerms(agg, nonNull, DocValueFormat.RAW);
        }
        return stringTerms(agg, nonNull);
    }

    private static InternalAggregation stringTerms(TermsAggregationBuilder agg, List<BucketEntry> entries) {
        List<StringTerms.Bucket> termBuckets = new ArrayList<>(entries.size());
        for (BucketEntry entry : entries) {
            BytesRef term = new BytesRef(keyString(entry.keys().get(0)));
            termBuckets.add(new StringTerms.Bucket(term, entry.docCount(), entry.subAggs(), false, 0, DocValueFormat.RAW));
        }
        BucketOrder order = agg.order();
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
            thresholds(agg)
        );
    }

    private static InternalAggregation longTerms(TermsAggregationBuilder agg, List<BucketEntry> entries, DocValueFormat format) {
        List<LongTerms.Bucket> termBuckets = new ArrayList<>(entries.size());
        for (BucketEntry entry : entries) {
            Object key = entry.keys().get(0);
            long term = key instanceof Boolean bool ? (bool ? 1L : 0L) : ((Number) key).longValue();
            termBuckets.add(new LongTerms.Bucket(term, entry.docCount(), entry.subAggs(), false, 0, format));
        }
        BucketOrder order = agg.order();
        return new LongTerms(agg.getName(), order, order, null, format, agg.shardSize(), false, 0, termBuckets, 0, thresholds(agg));
    }

    private static InternalAggregation doubleTerms(TermsAggregationBuilder agg, List<BucketEntry> entries) {
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
            null,
            DocValueFormat.RAW,
            agg.shardSize(),
            false,
            0,
            termBuckets,
            0,
            thresholds(agg)
        );
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

    private static TermsAggregator.BucketCountThresholds thresholds(TermsAggregationBuilder agg) {
        return new TermsAggregator.BucketCountThresholds(agg.minDocCount(), agg.shardMinDocCount(), agg.size(), agg.shardSize());
    }
}

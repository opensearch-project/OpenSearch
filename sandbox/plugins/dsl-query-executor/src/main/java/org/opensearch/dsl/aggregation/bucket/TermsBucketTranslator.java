/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.FieldGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Translates a {@link TermsAggregationBuilder} — single-field GROUP BY.
 * {@code {"aggs": {"by_brand": {"terms": {"field": "brand"}}}}} becomes {@code GROUP BY brand}.
 *
 * <p>Response typing is handled by the {@link TermsResponseStrategy} registry: the field's
 * mapping-resolved type name selects the strategy that builds the correct {@code InternalTerms}
 * subclass, with the mapping's {@link DocValueFormat} rendering the keys. When no mapping is
 * resolvable, typing falls back to sampling the first bucket key's Java type with RAW formats.
 */
public class TermsBucketTranslator implements SizedBucketTranslator<TermsAggregationBuilder> {

    private final Supplier<MapperService> mapperServiceSupplier;

    /**
     * Creates a terms bucket translator.
     *
     * @param mapperServiceSupplier supplies the target index's MapperService for key type and
     *        format resolution; may supply null, which selects the sampling fallback
     */
    public TermsBucketTranslator(Supplier<MapperService> mapperServiceSupplier) {
        this.mapperServiceSupplier = mapperServiceSupplier;
    }

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
     * Builds the terms response: the field's mapping selects the {@link TermsResponseStrategy}
     * and the {@link DocValueFormat} for key rendering; without a resolvable mapping, both are
     * inferred from the first bucket key's Java type. {@code eligibleDocCount} supplies the
     * total {@code sum_other_doc_count} is subtracted from (see {@link #sumOtherDocCount}).
     */
    private InternalAggregation render(TermsAggregationBuilder agg, List<BucketEntry> kept, long eligibleDocCount) {
        long otherDocCount = sumOtherDocCount(kept, eligibleDocCount);
        MappedFieldType fieldType = resolveFieldType(agg.field());
        if (fieldType != null) {
            TermsResponseStrategy strategy = TermsResponseStrategy.forType(fieldType.typeName());
            return strategy.build(agg, kept, otherDocCount, fieldType.docValueFormat(null, null));
        }
        return sampledStrategy(kept).build(agg, kept, otherDocCount, DocValueFormat.RAW);
    }

    /** Resolves the group field's mapping, or null when no MapperService is available. */
    private MappedFieldType resolveFieldType(String field) {
        MapperService mapperService = mapperServiceSupplier.get();
        return mapperService == null ? null : mapperService.fieldType(field);
    }

    /**
     * Mapping-less fallback: infers the strategy from the first bucket key's Java type —
     * booleans and integral numbers → LongTerms, floating point → DoubleTerms, anything
     * else (including binary ip keys) → StringTerms.
     */
    private static TermsResponseStrategy sampledStrategy(List<BucketEntry> kept) {
        Object sample = kept.isEmpty() ? null : kept.get(0).keys().get(0);
        if (sample instanceof Boolean) {
            return TermsResponseStrategy.forType("boolean");
        }
        if (sample instanceof Double || sample instanceof Float) {
            return TermsResponseStrategy.forType("double");
        }
        if (sample instanceof Number) {
            return TermsResponseStrategy.forType("long");
        }
        return TermsResponseStrategy.DEFAULT;
    }

    /**
     * Computes {@code sum_other_doc_count} — docs belonging to groups not in the bucket
     * list — as {@code eligibleDocCount − Σ(received doc counts)}, clamped at zero: root
     * eligible counts come from a separately executed COUNT plan, and a refresh landing between
     * the two queries can leave the eligible count smaller than the received sum. (Nested
     * eligible counts ride the plan's own rows and cannot skew.)
     */
    private static long sumOtherDocCount(List<BucketEntry> entries, long eligibleDocCount) {
        long receivedDocCount = 0;
        for (BucketEntry entry : entries) {
            receivedDocCount += entry.docCount();
        }
        return Math.max(0, eligibleDocCount - receivedDocCount);
    }

    /** Bundles the request's bucket-count knobs for the result constructors. */
    static TermsAggregator.BucketCountThresholds thresholds(TermsAggregationBuilder agg) {
        return new TermsAggregator.BucketCountThresholds(agg.minDocCount(), agg.shardMinDocCount(), agg.size(), agg.shardSize());
    }
}

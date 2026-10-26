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
import org.opensearch.index.mapper.DateFieldMapper;
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
 * <p>Response typing is mapping-resolved: the field's {@code MappedFieldType.typeName()} selects
 * the {@link TermsResponseStrategy} that builds the correct {@code InternalTerms} subclass, and
 * the mapping's {@link DocValueFormat} renders the keys. A field whose mapping cannot be
 * resolved at render time fails the request.
 */
public class TermsBucketTranslator implements SizedBucketTranslator<TermsAggregationBuilder> {

    private final Supplier<MapperService> mapperServiceSupplier;

    /**
     * Creates a terms bucket translator.
     *
     * @param mapperServiceSupplier supplies the target index's MapperService for key type and
     *        format resolution; supplying null skips {@link #validate} mapping checks and fails
     *        rendering
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
     * Rejects {@code include}/{@code exclude}, {@code script}, {@code min_doc_count: 0}, and
     * date-mapped fields: the translation implements none of them, and each would change the
     * bucket set or key rendering relative to classic search if ignored.
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
        MappedFieldType fieldType = resolveFieldType(agg.field());
        if (fieldType != null
            && (DateFieldMapper.CONTENT_TYPE.equals(fieldType.typeName())
                || DateFieldMapper.DATE_NANOS_CONTENT_TYPE.equals(fieldType.typeName()))) {
            throw new ConversionException(
                "terms aggregation ["
                    + agg.getName()
                    + "] on date field ["
                    + agg.field()
                    + "] is not supported by the DSL execution path — date bucket keys cannot yet be rendered with mapping formats"
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
     * and the {@link DocValueFormat} for key rendering. {@code eligibleDocCount} supplies the
     * total {@code sum_other_doc_count} is subtracted from (see {@link #sumOtherDocCount}).
     */
    private InternalAggregation render(TermsAggregationBuilder agg, List<BucketEntry> kept, long eligibleDocCount) {
        long otherDocCount = sumOtherDocCount(kept, eligibleDocCount);
        MappedFieldType fieldType = requireFieldType(agg);
        TermsResponseStrategy strategy = TermsResponseStrategy.forType(fieldType.typeName());
        return strategy.build(agg, kept, otherDocCount, fieldType.docValueFormat(null, null));
    }

    /** Resolves the group field's mapping, or null when the MapperService or field mapping is unavailable. */
    private MappedFieldType resolveFieldType(String field) {
        MapperService mapperService = mapperServiceSupplier.get();
        return mapperService == null ? null : mapperService.fieldType(field);
    }

    /** Resolves the group field's mapping for rendering, failing loudly when it cannot be resolved. */
    private MappedFieldType requireFieldType(TermsAggregationBuilder agg) {
        MapperService mapperService = mapperServiceSupplier.get();
        if (mapperService == null) {
            throw new IllegalStateException(
                "index mapping unavailable for terms aggregation ["
                    + agg.getName()
                    + "] — cannot resolve the key type for field ["
                    + agg.field()
                    + "]"
            );
        }
        MappedFieldType fieldType = mapperService.fieldType(agg.field());
        if (fieldType == null) {
            throw new IllegalStateException(
                "field [" + agg.field() + "] of terms aggregation [" + agg.getName() + "] is not present in the index mapping"
            );
        }
        return fieldType;
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.ExpressionGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.range.RangeAggregator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

/**
 * Translates a {@link RangeAggregationBuilder} — bucketing a numeric field into user-declared
 * half-open intervals {@code [from, to)}. Each document is mapped to a single range <b>ordinal</b>
 * by a computed group key (an {@link ExpressionGrouping}); {@code GROUP BY ordinal, COUNT(*)}
 * then yields one row per non-empty range, which {@link RangeResponseStrategy} renders back into
 * an {@code InternalRange} with every declared range present (empty ranges as {@code doc_count:0}).
 *
 * <p><b>Single-membership only.</b> Classic {@code range} lets ranges overlap and counts a
 * document in <em>every</em> matching range (see {@code RangeAggregator.collect}, which loops all
 * matching ranges and calls {@code collectBucket} for each). A one-ordinal-per-document GROUP BY
 * cannot reproduce that, so overlapping ranges are rejected in {@link #validate} rather than
 * silently under-counted. This mirrors PPL's {@code bin}, which is single-membership by design.
 * Non-overlapping (contiguous) ranges — the common case — are exact.
 */
public class RangeBucketTranslator implements BucketTranslator<RangeAggregationBuilder> {

    private final Supplier<MapperService> mapperServiceSupplier;

    /**
     * Creates a range bucket translator.
     *
     * @param mapperServiceSupplier supplies the target index's MapperService for the response
     *        {@link DocValueFormat}; evaluated lazily. Supplying null fails rendering.
     */
    public RangeBucketTranslator(Supplier<MapperService> mapperServiceSupplier) {
        this.mapperServiceSupplier = mapperServiceSupplier;
    }

    @Override
    public Class<RangeAggregationBuilder> getAggregationType() {
        return RangeAggregationBuilder.class;
    }

    /**
     * The group key is a synthetic ordinal column computed from the source field. Ordinal
     * {@code i} corresponds to {@code agg.ranges().get(i)} — declaration order — so the response
     * maps ordinals straight back to the declared ranges.
     */
    @Override
    public GroupingInfo getGrouping(RangeAggregationBuilder agg) {
        List<ExpressionGrouping.Bound> bounds = new ArrayList<>(agg.ranges().size());
        for (RangeAggregator.Range range : agg.ranges()) {
            bounds.add(new ExpressionGrouping.Bound(range.getFrom(), range.getTo()));
        }
        return new ExpressionGrouping(syntheticColumn(agg), agg.field(), bounds);
    }

    @Override
    public Collection<AggregationBuilder> getSubAggregations(RangeAggregationBuilder agg) {
        return agg.getSubAggregations();
    }

    /** Range buckets are returned in declaration order, never reordered — no bucket order. */
    @Override
    public BucketOrder getBucketOrder(RangeAggregationBuilder agg) {
        return null;
    }

    /**
     * Rejects parameters this single-membership path cannot honor without diverging from classic
     * results: {@code script} (no field to bucket), {@code missing} (would need a null-substituting
     * projection, not yet wired), and — the semantic gate — <b>overlapping ranges</b>, which
     * classic search counts as multi-membership.
     */
    @Override
    public void validate(RangeAggregationBuilder agg) throws ConversionException {
        if (agg.script() != null) {
            throw new ConversionException(
                "[script] on range aggregation [" + agg.getName() + "] is not supported by the DSL execution path"
            );
        }
        if (agg.missing() != null) {
            throw new ConversionException(
                "[missing] on range aggregation [" + agg.getName() + "] is not supported by the DSL execution path"
            );
        }
        rejectOverlappingRanges(agg);
    }

    /**
     * Fails when any two ranges overlap. Ranges are half-open {@code [from, to)}, so touching
     * bounds ({@code [0,100)} and {@code [100,200)}) do not overlap. Detection sweeps the ranges
     * sorted by {@code from} against the running maximum {@code to}: an overlap exists iff some
     * range starts strictly before the greatest {@code to} seen so far — the same
     * {@code from < maxTo} test classic search's {@code RangeAggregator} builds its {@code maxTo}
     * array for.
     */
    private static void rejectOverlappingRanges(RangeAggregationBuilder agg) throws ConversionException {
        List<RangeAggregator.Range> sorted = new ArrayList<>(agg.ranges());
        sorted.sort(Comparator.comparingDouble(RangeAggregator.Range::getFrom).thenComparingDouble(RangeAggregator.Range::getTo));
        double maxTo = Double.NEGATIVE_INFINITY;
        for (RangeAggregator.Range range : sorted) {
            if (range.getFrom() < maxTo) {
                throw new ConversionException(
                    "overlapping ranges on range aggregation ["
                        + agg.getName()
                        + "] are not supported by the DSL execution path — classic search counts a document in every "
                        + "matching range (RangeAggregator.collect), which single-ordinal grouping cannot reproduce"
                );
            }
            maxTo = Math.max(maxTo, range.getTo());
        }
    }

    @Override
    public InternalAggregation toBucketAggregation(RangeAggregationBuilder agg, Iterable<BucketEntry> buckets) {
        return RangeResponseStrategy.build(agg, buckets, resolveFormat(agg));
    }

    /** The synthetic ordinal column name; {@code $} cannot appear in a field name, so it cannot collide with a mapped field. */
    private static String syntheticColumn(RangeAggregationBuilder agg) {
        return "_range$" + agg.getName();
    }

    /** Resolves the source field's {@link DocValueFormat} for key rendering, failing loudly when the mapping is unavailable. */
    private DocValueFormat resolveFormat(RangeAggregationBuilder agg) {
        MapperService mapperService = mapperServiceSupplier.get();
        if (mapperService == null) {
            throw new IllegalStateException(
                "index mapping unavailable for range aggregation ["
                    + agg.getName()
                    + "] — cannot resolve the format for field ["
                    + agg.field()
                    + "]"
            );
        }
        MappedFieldType fieldType = mapperService.fieldType(agg.field());
        if (fieldType == null) {
            throw new IllegalStateException(
                "field [" + agg.field() + "] of range aggregation [" + agg.getName() + "] is not present in the index mapping"
            );
        }
        return fieldType.docValueFormat(agg.format(), null);
    }
}

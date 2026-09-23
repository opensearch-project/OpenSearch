/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumericPointEncoder;
import org.opensearch.search.aggregations.bucket.range.RangeAggregator;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * For range aggregation
 */
public abstract class RangeAggregatorBridge extends AggregatorBridge {

    protected boolean canOptimize(ValuesSourceConfig config, RangeAggregator.Range[] ranges) {
        // Runtime path: FilterRewriteOptimizationContext#canOptimize already gates on parent == null before
        // reaching here, so parent is passed as null.
        if (filterRewriteFastPathApplies(null, config, ranges)) {
            this.fieldType = config.fieldType();
            return true;
        }
        return false;
    }

    /**
     * Whether the range filter-rewrite fast path applies for this aggregation. The single source of truth for
     * the fast-path preconditions — top-level only ({@code parent == null}), searchable numeric field, no
     * script/missing, and non-overlapping ranges. Used both at runtime ({@link #canOptimize}) and by the range
     * aggregator factory to decide intra-segment eligibility: intra-segment search is used only when this
     * returns false (the fast path is unavailable and the doc-by-doc fallback, which parallelizes, runs).
     */
    public static boolean filterRewriteFastPathApplies(Object parent, ValuesSourceConfig config, RangeAggregator.Range[] ranges) {
        // The fast path (BKD point-tree precompute) only runs for a top-level agg; nested aggs collect
        // doc-by-doc under their parent's buckets.
        if (parent != null) {
            return false;
        }
        MappedFieldType fieldType = config.fieldType();
        if (fieldType == null || fieldType.isSearchable() == false || !(fieldType instanceof NumericPointEncoder)) {
            return false;
        }
        if (config.script() != null || config.missing() != null) {
            return false;
        }
        if ((config.getValuesSource() instanceof ValuesSource.Numeric.FieldData) == false) {
            return false;
        }
        // ranges are already sorted by from and then to; the fast path requires non-overlapping ranges
        double prevTo = ranges[0].getTo();
        for (int i = 1; i < ranges.length; i++) {
            if (prevTo > ranges[i].getFrom()) {
                return false;
            }
            prevTo = ranges[i].getTo();
        }
        return true;
    }

    protected void buildRanges(RangeAggregator.Range[] ranges) {
        assert fieldType instanceof NumericPointEncoder;
        NumericPointEncoder numericPointEncoder = (NumericPointEncoder) fieldType;
        byte[][] lowers = new byte[ranges.length][];
        byte[][] uppers = new byte[ranges.length][];
        for (int i = 0; i < ranges.length; i++) {
            double rangeMin = ranges[i].getFrom();
            double rangeMax = ranges[i].getTo();
            byte[] lower = numericPointEncoder.encodePoint(rangeMin);
            byte[] upper = numericPointEncoder.encodePoint(rangeMax);
            lowers[i] = lower;
            uppers[i] = upper;
        }

        setRanges.accept(new Ranges(lowers, uppers));
    }

    @Override
    final Ranges tryBuildRangesFromSegment(LeafReaderContext leaf) {
        throw new UnsupportedOperationException("Range aggregation should not build ranges at segment level");
    }

    @Override
    final FilterRewriteOptimizationContext.OptimizeResult tryOptimize(
        PointValues values,
        BiConsumer<Long, Long> incrementDocCount,
        Ranges ranges,
        FilterRewriteOptimizationContext.SubAggCollectorParam subAggCollectorParam
    ) throws IOException {
        int size = Integer.MAX_VALUE;

        Function<Integer, Long> getBucketOrd = (activeIndex) -> bucketOrdProducer().apply(activeIndex);

        return getResult(values, incrementDocCount, ranges, getBucketOrd, size, subAggCollectorParam);
    }

    /**
     * Provides a function to produce bucket ordinals from index of the corresponding range in the range array
     */
    protected abstract Function<Object, Long> bucketOrdProducer();
}

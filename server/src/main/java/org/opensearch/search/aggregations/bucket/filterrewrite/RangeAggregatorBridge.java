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
        if (filterRewriteFastPathApplies(config, ranges)) {
            this.fieldType = config.fieldType();
            return true;
        }
        return false;
    }

    /**
     * Request-level (segment-independent) predicate for whether the range filter-rewrite fast path can apply.
     * Pure (no side effects) so it can be consulted upfront by the range aggregator factory to decide
     * intra-segment eligibility: when this returns false the fast path is unavailable and the doc-by-doc
     * fallback — which parallelizes under intra-segment search — will run instead. Kept in sync with the
     * conditions {@link #canOptimize(ValuesSourceConfig, RangeAggregator.Range[])} enforces.
     */
    public static boolean filterRewriteFastPathApplies(ValuesSourceConfig config, RangeAggregator.Range[] ranges) {
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

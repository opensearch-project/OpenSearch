/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.optimization.ranges;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumericPointEncoder;
import org.opensearch.search.aggregations.bucket.range.RangeAggregator;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.opensearch.search.optimization.ranges.OptimizationContext.multiRangesTraverse;

/**
 * For range aggregation
 */
public abstract class AbstractRangeAggregatorDataProvider implements AggregatorDataProvider {

    public boolean canOptimize(ValuesSourceConfig config, RangeAggregator.Range[] ranges) {
        if (config.fieldType() == null) return false;
        MappedFieldType fieldType = config.fieldType();
        if (fieldType.isSearchable() == false || !(fieldType instanceof NumericPointEncoder)) return false;

        if (config.script() == null && config.missing() == null) {
            if (config.getValuesSource() instanceof ValuesSource.Numeric.FieldData) {
                // ranges are already sorted by from and then to
                // we want ranges not overlapping with each other
                double prevTo = ranges[0].getTo();
                for (int i = 1; i < ranges.length; i++) {
                    if (prevTo > ranges[i].getFrom()) {
                        return false;
                    }
                    prevTo = ranges[i].getTo();
                }
                return true;
            }
        }
        return false;
    }

    public OptimizationContext.Ranges buildRanges(MappedFieldType fieldType, RangeAggregator.Range[] ranges) {
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

        return new OptimizationContext.Ranges(lowers, uppers);
    }

    @Override
    public OptimizationContext.Ranges buildRanges(LeafReaderContext leaf, SearchContext ctx, MappedFieldType fieldType) {
        throw new UnsupportedOperationException("Range aggregation should not build ranges at segment level");
    }

    @Override
    public OptimizationContext.DebugInfo tryFastFilterAggregation(
        PointValues values,
        OptimizationContext.Ranges ranges,
        BiConsumer<Long, Long> incrementDocCount,
        Function<Object, Long> bucketOrd
    ) throws IOException {
        int size = Integer.MAX_VALUE;

        BiConsumer<Integer, Integer> incrementFunc = (activeIndex, docCount) -> {
            long ord = bucketOrd.apply(activeIndex);
            incrementDocCount.accept(ord, (long) docCount);
        };

        return multiRangesTraverse(values.getPointTree(), ranges, incrementFunc, size);
    }
}

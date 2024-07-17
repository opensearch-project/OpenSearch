/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.optimization.filterrewrite;

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

import static org.opensearch.search.optimization.filterrewrite.TreeTraversal.multiRangesTraverse;

/**
 * For range aggregation
 */
public abstract class RangeAggregatorBridge extends AggregatorBridge {

    protected boolean canOptimize(ValuesSourceConfig config, RangeAggregator.Range[] ranges) {
        if (config.fieldType() == null) return false;
        MappedFieldType fieldType = config.fieldType();
        assert fieldType != null;
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
                this.fieldType = config.fieldType();
                return true;
            }
        }
        return false;
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

        optimizationContext.setRanges(new Ranges(lowers, uppers));
    }

    @Override
    public void prepareFromSegment(LeafReaderContext leaf) {
        throw new UnsupportedOperationException("Range aggregation should not build ranges at segment level");
    }

    @Override
    public final void tryOptimize(PointValues values, BiConsumer<Long, Long> incrementDocCount) throws IOException {
        int size = Integer.MAX_VALUE;

        BiConsumer<Integer, Integer> incrementFunc = (activeIndex, docCount) -> {
            long ord = bucketOrdProducer().apply(activeIndex);
            incrementDocCount.accept(ord, (long) docCount);
        };

        optimizationContext.consumeDebugInfo(
            multiRangesTraverse(values.getPointTree(), optimizationContext.getRanges(), incrementFunc, size)
        );
    }

    /**
     * Provides a function to produce bucket ordinals from index of the corresponding range in the range array
     */
    protected abstract Function<Object, Long> bucketOrdProducer();
}

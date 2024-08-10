/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumericPointEncoder;
import org.opensearch.search.aggregations.bucket.range.RangeAggregator;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;

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

        setRanges.accept(new PackedValueRanges(lowers, uppers));
    }

    @Override
    final PackedValueRanges tryBuildRangesFromSegment(LeafReaderContext leaf) {
        throw new UnsupportedOperationException("Range aggregation should not build ranges at segment level");
    }
}

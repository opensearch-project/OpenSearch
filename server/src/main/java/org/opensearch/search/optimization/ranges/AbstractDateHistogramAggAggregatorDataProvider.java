/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.optimization.ranges;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.opensearch.common.Rounding;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.OptionalLong;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.opensearch.search.optimization.ranges.OptimizationContext.multiRangesTraverse;

/**
 * For date histogram aggregation
 */
public abstract class AbstractDateHistogramAggAggregatorDataProvider implements AggregatorDataProvider {
    private MappedFieldType fieldType;

    public boolean canOptimize(boolean missing, boolean hasScript, MappedFieldType fieldType) {
        if (!missing && !hasScript) {
            if (fieldType != null && fieldType instanceof DateFieldMapper.DateFieldType) {
                if (fieldType.isSearchable()) {
                    this.fieldType = fieldType;
                    return true;
                }
            }
        }
        return false;
    }

    public boolean canOptimize(ValuesSourceConfig config) {
        if (config.script() == null && config.missing() == null) {
            MappedFieldType fieldType = config.fieldType();
            if (fieldType instanceof DateFieldMapper.DateFieldType) {
                if (fieldType.isSearchable()) {
                    this.fieldType = fieldType;
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public OptimizationContext.Ranges buildRanges(SearchContext context, MappedFieldType fieldType) throws IOException {
        long[] bounds = Helper.getDateHistoAggBounds(context, fieldType.name());
        // logger.debug("Bounds are {} for shard {}", bounds, context.indexShard().shardId());
        return buildRanges(context, bounds);
    }

    @Override
    public OptimizationContext.Ranges buildRanges(LeafReaderContext leaf, SearchContext context, MappedFieldType fieldType)
        throws IOException {
        long[] bounds = Helper.getSegmentBounds(leaf, fieldType.name());
        // logger.debug("Bounds are {} for shard {} segment {}", bounds, context.indexShard().shardId(), leaf.ord);
        return buildRanges(context, bounds);
    }

    private OptimizationContext.Ranges buildRanges(SearchContext context, long[] bounds) throws IOException {
        bounds = processHardBounds(bounds);
        if (bounds == null) {
            return null;
        }
        assert bounds[0] <= bounds[1] : "Low bound should be less than high bound";

        final Rounding rounding = getRounding(bounds[0], bounds[1]);
        final OptionalLong intervalOpt = Rounding.getInterval(rounding);
        if (intervalOpt.isEmpty()) {
            return null;
        }
        final long interval = intervalOpt.getAsLong();

        // process the after key of composite agg
        bounds = processAfterKey(bounds, interval);

        return Helper.createRangesFromAgg(
            context,
            (DateFieldMapper.DateFieldType) fieldType,
            interval,
            getRoundingPrepared(),
            bounds[0],
            bounds[1]
        );
    }

    protected abstract Rounding getRounding(final long low, final long high);

    protected abstract Rounding.Prepared getRoundingPrepared();

    protected long[] processAfterKey(long[] bounds, long interval) {
        return bounds;
    }

    protected long[] processHardBounds(long[] bounds) {
        return processHardBounds(bounds, null);
    }

    protected long[] processHardBounds(long[] bounds, LongBounds hardBounds) {
        if (bounds != null) {
            // Update min/max limit if user specified any hard bounds
            if (hardBounds != null) {
                if (hardBounds.getMin() > bounds[0]) {
                    bounds[0] = hardBounds.getMin();
                }
                if (hardBounds.getMax() - 1 < bounds[1]) {
                    bounds[1] = hardBounds.getMax() - 1; // hard bounds max is exclusive
                }
                if (bounds[0] > bounds[1]) {
                    return null;
                }
            }
        }
        return bounds;
    }

    public DateFieldMapper.DateFieldType getFieldType() {
        assert fieldType instanceof DateFieldMapper.DateFieldType;
        return (DateFieldMapper.DateFieldType) fieldType;
    }

    public int getSize() {
        return Integer.MAX_VALUE;
    }

    @Override
    public OptimizationContext.DebugInfo tryFastFilterAggregation(
        PointValues values,
        OptimizationContext.Ranges ranges,
        BiConsumer<Long, Long> incrementDocCount,
        Function<Object, Long> bucketOrd
    ) throws IOException {
        int size = getSize();

        DateFieldMapper.DateFieldType fieldType = getFieldType();
        BiConsumer<Integer, Integer> incrementFunc = (activeIndex, docCount) -> {
            long rangeStart = LongPoint.decodeDimension(ranges.lowers[activeIndex], 0);
            rangeStart = fieldType.convertNanosToMillis(rangeStart);
            long ord = getBucketOrd(bucketOrd.apply(rangeStart));
            incrementDocCount.accept(ord, (long) docCount);
        };

        return multiRangesTraverse(values.getPointTree(), ranges, incrementFunc, size);
    }

    private static long getBucketOrd(long bucketOrd) {
        if (bucketOrd < 0) { // already seen
            bucketOrd = -1 - bucketOrd;
        }

        return bucketOrd;
    }
}

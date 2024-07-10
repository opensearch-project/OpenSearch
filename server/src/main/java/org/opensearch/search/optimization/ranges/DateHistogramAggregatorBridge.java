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
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.opensearch.common.Rounding;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceConfig;
import org.opensearch.search.aggregations.bucket.composite.RoundingValuesSource;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.OptionalLong;
import java.util.function.BiConsumer;

import static org.opensearch.search.optimization.ranges.Helper.multiRangesTraverse;

/**
 * For date histogram aggregation
 */
public abstract class DateHistogramAggregatorBridge extends AggregatorBridge {

    protected boolean canOptimize(boolean missing, boolean hasScript, MappedFieldType fieldType) {
        if (!missing && !hasScript) {
            if (fieldType instanceof DateFieldMapper.DateFieldType) {
                if (fieldType.isSearchable()) {
                    this.fieldType = fieldType;
                    return true;
                }
            }
        }
        return false;
    }

    protected boolean canOptimize(ValuesSourceConfig config) {
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

    protected boolean canOptimize(CompositeValuesSourceConfig[] sourceConfigs) {
        if (sourceConfigs.length != 1 || !(sourceConfigs[0].valuesSource() instanceof RoundingValuesSource)) return false;
        return canOptimize(sourceConfigs[0].missingBucket(), sourceConfigs[0].hasScript(), sourceConfigs[0].fieldType());
    }

    protected void buildRanges(SearchContext context) throws IOException {
        long[] bounds = Helper.getDateHistoAggBounds(context, fieldType.name());
        optimizationContext.setRanges(buildRanges(bounds));
    }

    @Override
    public void prepareFromSegment(LeafReaderContext leaf) throws IOException {
        long[] bounds = Helper.getSegmentBounds(leaf, fieldType.name());
        optimizationContext.setRangesFromSegment(buildRanges(bounds));
    }

    private Ranges buildRanges(long[] bounds) {
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
            (DateFieldMapper.DateFieldType) fieldType,
            interval,
            getRoundingPrepared(),
            bounds[0],
            bounds[1],
            optimizationContext.maxAggRewriteFilters
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

    private DateFieldMapper.DateFieldType getFieldType() {
        assert fieldType instanceof DateFieldMapper.DateFieldType;
        return (DateFieldMapper.DateFieldType) fieldType;
    }

    protected int getSize() {
        return Integer.MAX_VALUE;
    }

    @Override
    public final void tryOptimize(PointValues values, BiConsumer<Long, Long> incrementDocCount) throws IOException {
        int size = getSize();

        DateFieldMapper.DateFieldType fieldType = getFieldType();
        BiConsumer<Integer, Integer> incrementFunc = (activeIndex, docCount) -> {
            long rangeStart = LongPoint.decodeDimension(optimizationContext.getRanges().lowers[activeIndex], 0);
            rangeStart = fieldType.convertNanosToMillis(rangeStart);
            long ord = getBucketOrd(bucketOrdProducer().apply(rangeStart));
            incrementDocCount.accept(ord, (long) docCount);
        };

        optimizationContext.consumeDebugInfo(
            multiRangesTraverse(values.getPointTree(), optimizationContext.getRanges(), incrementFunc, size)
        );
    }

    private static long getBucketOrd(long bucketOrd) {
        if (bucketOrd < 0) { // already seen
            bucketOrd = -1 - bucketOrd;
        }

        return bucketOrd;
    }

    protected boolean segmentMatchAll(SearchContext ctx, LeafReaderContext leafCtx) throws IOException {
        Weight weight = ctx.query().rewrite(ctx.searcher()).createWeight(ctx.searcher(), ScoreMode.COMPLETE_NO_SCORES, 1f);
        return weight != null && weight.count(leafCtx) == leafCtx.reader().numDocs();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import org.opensearch.common.Rounding;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.OptionalLong;

/**
 * For date histogram aggregation
 */
public abstract class DateHistogramAggregatorBridge extends AggregatorBridge {

    int maxRewriteFilters;

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

    protected void buildRanges(SearchContext context) throws IOException {
        long[] bounds = Helper.getDateHistoAggBounds(context, fieldType.name());
        this.maxRewriteFilters = context.maxAggRewriteFilters();
        setRanges.accept(buildRanges(bounds, maxRewriteFilters));
    }

    @Override
    final PackedValueRanges tryBuildRangesFromSegment(LeafReaderContext leaf) throws IOException {
        long[] bounds = Helper.getSegmentBounds(leaf, fieldType.name());
        return buildRanges(bounds, maxRewriteFilters);
    }

    private PackedValueRanges buildRanges(long[] bounds, int maxRewriteFilters) {
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
            maxRewriteFilters
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

    /**
     * Checks whether the top level query matches all documents on the segment
     *
     * <p>This method creates a weight from the search context's query and checks whether the weight's
     * document count matches the total number of documents in the leaf reader context.
     *
     * @param ctx      the search context
     * @param leafCtx  the leaf reader context for the segment
     * @return {@code true} if the segment matches all documents, {@code false} otherwise
     */
    public static boolean segmentMatchAll(SearchContext ctx, LeafReaderContext leafCtx) throws IOException {
        Weight weight = ctx.query().rewrite(ctx.searcher()).createWeight(ctx.searcher(), ScoreMode.COMPLETE_NO_SCORES, 1f);
        return weight != null && weight.count(leafCtx) == leafCtx.reader().numDocs();
    }
}

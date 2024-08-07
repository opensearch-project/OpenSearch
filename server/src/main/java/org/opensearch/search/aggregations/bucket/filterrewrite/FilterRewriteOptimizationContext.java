/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.function.BiConsumer;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Context object for doing the filter rewrite optimization in ranges type aggregation
 * <p>
 * This holds the common business logic and delegate aggregator-specific logic to {@link AggregatorBridge}
 *
 * @opensearch.internal
 */
public final class FilterRewriteOptimizationContext {

    private static final Logger logger = LogManager.getLogger(Helper.loggerName);

    private final boolean canOptimize;
    private boolean preparedAtShardLevel = false;

    final AggregatorBridge aggregatorBridge;
    int maxAggRewriteFilters;
    String shardId;

    private Ranges ranges;
    private Ranges rangesFromSegment;

    // debug info related fields
    private int leaf;
    private int inner;
    private int segments;
    private int optimizedSegments;

    public FilterRewriteOptimizationContext(
        AggregatorBridge aggregatorBridge,
        final Object parent,
        final int subAggLength,
        SearchContext context
    ) throws IOException {
        this.aggregatorBridge = aggregatorBridge;
        this.canOptimize = this.canOptimize(parent, subAggLength, context);
    }

    private boolean canOptimize(final Object parent, final int subAggLength, SearchContext context) throws IOException {
        if (context.maxAggRewriteFilters() == 0) return false;

        if (parent != null || subAggLength != 0) return false;

        boolean canOptimize = aggregatorBridge.canOptimize();
        if (canOptimize) {
            aggregatorBridge.setOptimizationContext(this);
            this.maxAggRewriteFilters = context.maxAggRewriteFilters();
            this.shardId = context.indexShard().shardId().toString();
            this.prepare();
        }
        logger.debug("Fast filter rewriteable: {} for shard {}", canOptimize, shardId);

        return canOptimize;
    }

    private void prepare() throws IOException {
        assert ranges == null : "Ranges should only be built once at shard level, but they are already built";
        aggregatorBridge.prepare();
        if (ranges != null) {
            preparedAtShardLevel = true;
        }
    }

    void setRanges(Ranges ranges) {
        this.ranges = ranges;
    }

    void setRangesFromSegment(Ranges ranges) {
        this.rangesFromSegment = ranges;
    }

    Ranges getRanges() {
        if (rangesFromSegment != null) return rangesFromSegment;
        return ranges;
    }

    /**
     * Try to populate the bucket doc counts for aggregation
     * <p>
     * Usage: invoked at segment level — in getLeafCollector of aggregator
     *
     * @param incrementDocCount consume the doc_count results for certain ordinal
     * @param segmentMatchAll if your optimization can prepareFromSegment, you should pass in this flag to decide whether to prepareFromSegment
     */
    public boolean tryOptimize(final LeafReaderContext leafCtx, final BiConsumer<Long, Long> incrementDocCount, boolean segmentMatchAll)
        throws IOException {
        segments++;
        if (!canOptimize) {
            return false;
        }

        if (leafCtx.reader().hasDeletions()) return false;

        PointValues values = leafCtx.reader().getPointValues(aggregatorBridge.fieldType.name());
        if (values == null) return false;
        // only proceed if every document corresponds to exactly one point
        if (values.getDocCount() != values.size()) return false;

        NumericDocValues docCountValues = DocValues.getNumeric(leafCtx.reader(), DocCountFieldMapper.NAME);
        if (docCountValues.nextDoc() != NO_MORE_DOCS) {
            logger.debug(
                "Shard {} segment {} has at least one document with _doc_count field, skip fast filter optimization",
                shardId,
                leafCtx.ord
            );
            return false;
        }

        Ranges ranges = tryBuildRangesFromSegment(leafCtx, segmentMatchAll);
        if (ranges == null) return false;

        aggregatorBridge.tryOptimize(values, incrementDocCount);

        optimizedSegments++;
        logger.debug("Fast filter optimization applied to shard {} segment {}", shardId, leafCtx.ord);
        logger.debug("crossed leaf nodes: {}, inner nodes: {}", leaf, inner);

        rangesFromSegment = null;
        return true;
    }

    /**
     * Even when ranges cannot be built at shard level, we can still build ranges
     * at segment level when it's functionally match-all at segment level
     */
    private Ranges tryBuildRangesFromSegment(LeafReaderContext leafCtx, boolean segmentMatchAll) throws IOException {
        if (!preparedAtShardLevel && !segmentMatchAll) {
            return null;
        }

        if (ranges == null) { // not built at shard level but segment match all
            logger.debug("Shard {} segment {} functionally match all documents. Build the fast filter", shardId, leafCtx.ord);
            aggregatorBridge.prepareFromSegment(leafCtx);
            return rangesFromSegment;
        }
        return ranges;
    }

    /**
     * Contains debug info of BKD traversal to show in profile
     */
    static class DebugInfo {
        private int leaf = 0; // leaf node visited
        private int inner = 0; // inner node visited

        void visitLeaf() {
            leaf++;
        }

        void visitInner() {
            inner++;
        }
    }

    void consumeDebugInfo(DebugInfo debug) {
        leaf += debug.leaf;
        inner += debug.inner;
    }

    public void populateDebugInfo(BiConsumer<String, Object> add) {
        if (optimizedSegments > 0) {
            add.accept("optimized_segments", optimizedSegments);
            add.accept("unoptimized_segments", segments - optimizedSegments);
            add.accept("leaf_visited", leaf);
            add.accept("inner_visited", inner);
        }
    }
}

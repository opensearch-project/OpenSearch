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
import java.util.HashMap;
import java.util.Map;
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

    private final AggregatorBridge aggregatorBridge;
    private String shardId;

    private Ranges ranges;
    private final Map<Integer, Ranges> rangesFromSegment = new HashMap<>(); // map of segment ordinal to its ranges

    // debug info related fields
    private int leafNodeVisited;
    private int innerNodeVisited;
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

    /**
     * common logic for checking whether the optimization can be applied and prepare at shard level
     * if the aggregation has any special logic, it should be done using {@link AggregatorBridge}
     */
    private boolean canOptimize(final Object parent, final int subAggLength, SearchContext context) throws IOException {
        if (context.maxAggRewriteFilters() == 0) return false;

        if (parent != null || subAggLength != 0) return false;

        boolean canOptimize = aggregatorBridge.canOptimize();
        if (canOptimize) {
            aggregatorBridge.setRangesConsumer(this::setRanges);

            this.shardId = context.indexShard().shardId().toString();

            assert ranges == null : "Ranges should only be built once at shard level, but they are already built";
            aggregatorBridge.prepare();
            if (ranges != null) {
                preparedAtShardLevel = true;
            }
        }
        logger.debug("Fast filter rewriteable: {} for shard {}", canOptimize, shardId);

        return canOptimize;
    }

    void setRanges(Ranges ranges) {
        this.ranges = ranges;
    }

    void setRangesFromSegment(int leafOrd, Ranges ranges) {
        this.rangesFromSegment.put(leafOrd, ranges);
    }

    void clearRangesFromSegment(int leafOrd) {
        this.rangesFromSegment.remove(leafOrd);
    }

    Ranges getRanges(int leafOrd) {
        if (!preparedAtShardLevel) return rangesFromSegment.get(leafOrd);
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

        consumeDebugInfo(aggregatorBridge.tryOptimize(values, incrementDocCount, getRanges(leafCtx.ord)));

        optimizedSegments++;
        logger.debug("Fast filter optimization applied to shard {} segment {}", shardId, leafCtx.ord);
        logger.debug("Crossed leaf nodes: {}, inner nodes: {}", leafNodeVisited, innerNodeVisited);

        clearRangesFromSegment(leafCtx.ord);
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

        if (!preparedAtShardLevel) { // not built at shard level but segment match all
            logger.debug("Shard {} segment {} functionally match all documents. Build the fast filter", shardId, leafCtx.ord);
            setRangesFromSegment(leafCtx.ord, aggregatorBridge.prepareFromSegment(leafCtx));
        }
        return getRanges(leafCtx.ord);
    }

    /**
     * Contains debug info of BKD traversal to show in profile
     */
    static class DebugInfo {
        private int leafNodeVisited = 0; // leaf node visited
        private int innerNodeVisited = 0; // inner node visited

        void visitLeaf() {
            leafNodeVisited++;
        }

        void visitInner() {
            innerNodeVisited++;
        }
    }

    void consumeDebugInfo(DebugInfo debug) {
        leafNodeVisited += debug.leafNodeVisited;
        innerNodeVisited += debug.innerNodeVisited;
    }

    public void populateDebugInfo(BiConsumer<String, Object> add) {
        if (optimizedSegments > 0) {
            add.accept("optimized_segments", optimizedSegments);
            add.accept("unoptimized_segments", segments - optimizedSegments);
            add.accept("leaf_visited", leafNodeVisited);
            add.accept("inner_visited", innerNodeVisited);
        }
    }
}

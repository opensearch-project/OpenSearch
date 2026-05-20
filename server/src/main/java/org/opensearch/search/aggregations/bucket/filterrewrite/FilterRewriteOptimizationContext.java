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
import org.apache.lucene.util.DocIdSetBuilder;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.search.aggregations.BucketCollector;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
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

    private Ranges ranges; // built at shard level

    private boolean hasSubAgg;

    // debug info related fields
    private final AtomicInteger leafNodeVisited = new AtomicInteger();
    private final AtomicInteger innerNodeVisited = new AtomicInteger();
    private final AtomicInteger segments = new AtomicInteger();
    private final AtomicInteger optimizedSegments = new AtomicInteger();

    private int segmentThreshold = 0;

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

        if (parent != null) return false;
        this.hasSubAgg = subAggLength > 0;

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

        segmentThreshold = context.filterRewriteSegmentThreshold();
        return canOptimize;
    }

    void setRanges(Ranges ranges) {
        this.ranges = ranges;
    }

    /**
     * Try to populate the bucket doc counts for aggregation
     * <p>
     * Usage: invoked at segment level — in getLeafCollector of aggregator
     *
     * @param incrementDocCount consume the doc_count results for certain ordinal
     * @param segmentMatchAll   we can always tryOptimize for match all scenario
     */
    public boolean tryOptimize(
        final LeafReaderContext leafCtx,
        final BiConsumer<Long, Long> incrementDocCount,
        boolean segmentMatchAll,
        BucketCollector collectableSubAggregators
    ) throws IOException {
        segments.incrementAndGet();
        if (!canOptimize) {
            return false;
        }

        // Since we explicitly create bitset of matching docIds for each bucket
        // in case of sub-aggregations, deleted documents can be filtered out
        if (leafCtx.reader().hasDeletions() && hasSubAgg == false) return false;

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

        Ranges ranges = getRanges(leafCtx, segmentMatchAll);
        if (ranges == null) return false;

        if (hasSubAgg && this.segmentThreshold > leafCtx.reader().maxDoc() / ranges.getSize()) {
            // comparing with a rough estimate of docs per range in this segment
            return false;
        }

        OptimizeResult optimizeResult;
        SubAggCollectorParam subAggCollectorParam;
        if (hasSubAgg) {
            subAggCollectorParam = new SubAggCollectorParam(collectableSubAggregators, leafCtx);
        } else {
            subAggCollectorParam = null;
        }
        try {
            optimizeResult = aggregatorBridge.tryOptimize(values, incrementDocCount, ranges, subAggCollectorParam);
            consumeDebugInfo(optimizeResult);
        } catch (AbortFilterRewriteOptimizationException e) {
            logger.error("Abort filter rewrite optimization, fall back to default path");
            return false;
        }

        optimizedSegments.incrementAndGet();
        logger.debug("Fast filter optimization applied to shard {} segment {}", shardId, leafCtx.ord);
        logger.debug("Crossed leaf nodes: {}, inner nodes: {}", leafNodeVisited, innerNodeVisited);

        return true;
    }

    /**
     * Parameters for {@link org.opensearch.search.aggregations.bucket.filterrewrite.rangecollector.SubAggRangeCollector}
     */
    public record SubAggCollectorParam(BucketCollector collectableSubAggregators, LeafReaderContext leafCtx) {
    }

    static class AbortFilterRewriteOptimizationException extends RuntimeException {
        AbortFilterRewriteOptimizationException(String message, Exception e) {
            super(message, e);
        }
    }

    Ranges getRanges(LeafReaderContext leafCtx, boolean segmentMatchAll) {
        if (!preparedAtShardLevel) {
            try {
                return getRangesFromSegment(leafCtx, segmentMatchAll);
            } catch (IOException e) {
                logger.warn("Failed to build ranges from segment.", e);
                return null;
            }
        }
        return ranges;
    }

    /**
     * Even when ranges cannot be built at shard level, we can still build ranges
     * at segment level when it's functionally match-all at segment level
     */
    private Ranges getRangesFromSegment(LeafReaderContext leafCtx, boolean segmentMatchAll) throws IOException {
        if (!segmentMatchAll) {
            return null;
        }

        logger.debug("Shard {} segment {} functionally match all documents. Build the fast filter", shardId, leafCtx.ord);
        return aggregatorBridge.tryBuildRangesFromSegment(leafCtx);
    }

    /**
     * Contains debug info of BKD traversal to show in profile
     */
    public static class OptimizeResult {
        private final AtomicInteger leafNodeVisited = new AtomicInteger(); // leaf node visited
        private final AtomicInteger innerNodeVisited = new AtomicInteger(); // inner node visited

        public DocIdSetBuilder[] builders = new DocIdSetBuilder[0];

        public void visitLeaf() {
            leafNodeVisited.incrementAndGet();
        }

        public void visitInner() {
            innerNodeVisited.incrementAndGet();
        }
    }

    void consumeDebugInfo(OptimizeResult debug) {
        leafNodeVisited.addAndGet(debug.leafNodeVisited.get());
        innerNodeVisited.addAndGet(debug.innerNodeVisited.get());
    }

    public void populateDebugInfo(BiConsumer<String, Object> add) {
        if (optimizedSegments.get() > 0) {
            add.accept("optimized_segments", optimizedSegments.get());
            add.accept("unoptimized_segments", segments.get() - optimizedSegments.get());
            add.accept("leaf_visited", leafNodeVisited.get());
            add.accept("inner_visited", innerNodeVisited.get());
        }
    }
}

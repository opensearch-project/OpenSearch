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
import org.opensearch.search.aggregations.LeafBucketCollector;
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

    private PackedValueRanges ranges; // built at shard level

    // debug info related fields
    private final AtomicInteger leafNodeVisited = new AtomicInteger();
    private final AtomicInteger innerNodeVisited = new AtomicInteger();
    private final AtomicInteger segments = new AtomicInteger();
    private final AtomicInteger optimizedSegments = new AtomicInteger();

    public FilterRewriteOptimizationContext(AggregatorBridge aggregatorBridge, final Object parent, SearchContext context)
        throws IOException {
        this.aggregatorBridge = aggregatorBridge;
        this.canOptimize = this.canOptimize(parent, context);
    }

    /**
     * common logic for checking whether the optimization can be applied and prepare at shard level
     * if the aggregation has any special logic, it should be done using {@link AggregatorBridge}
     */
    private boolean canOptimize(final Object parent, SearchContext context) throws IOException {
        if (context.maxAggRewriteFilters() == 0) return false;

        if (parent != null) return false;

        boolean canOptimize = aggregatorBridge.canOptimize();
        if (canOptimize) {
            aggregatorBridge.setRangesConsumer((PackedValueRanges ranges) -> {
                this.ranges = ranges;
            });

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

    /**
     * Try to populate the bucket doc counts for aggregation
     * <p>
     * Usage: invoked at segment level — in getLeafCollector of aggregator
     *
     * @param incrementDocCount consume the doc_count results for certain ordinal
     * @param segmentMatchAll if your optimization can prepareFromSegment, you should pass in this flag to decide whether to prepareFromSegment
     */
    public boolean tryOptimize(
        final LeafReaderContext leafCtx,
        final BiConsumer<Long, Long> incrementDocCount,
        LeafBucketCollector sub,
        boolean segmentMatchAll
    ) throws IOException {
        segments.incrementAndGet();
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

        PackedValueRanges ranges = getRanges(leafCtx, segmentMatchAll);
        if (ranges == null) return false;

        consumeDebugInfo(aggregatorBridge.tryOptimize(values, incrementDocCount, ranges, sub));

        optimizedSegments.incrementAndGet();
        logger.debug("Fast filter optimization applied to shard {} segment {}", shardId, leafCtx.ord);
        logger.debug("Crossed leaf nodes: {}, inner nodes: {}", leafNodeVisited, innerNodeVisited);

        return true;
    }

    public PackedValueRanges getRanges(LeafReaderContext leafCtx, boolean segmentMatchAll) {
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
    private PackedValueRanges getRangesFromSegment(LeafReaderContext leafCtx, boolean segmentMatchAll) throws IOException {
        if (!segmentMatchAll) {
            return null;
        }

        logger.debug("Shard {} segment {} functionally match all documents. Build the fast filter", shardId, leafCtx.ord);
        return aggregatorBridge.tryBuildRangesFromSegment(leafCtx);
    }

    /**
     * Contains debug info of BKD traversal to show in profile
     */
    public static class DebugInfo {
        private final AtomicInteger leafNodeVisited = new AtomicInteger(); // leaf node visited
        private final AtomicInteger innerNodeVisited = new AtomicInteger(); // inner node visited

        void visitLeaf() {
            leafNodeVisited.incrementAndGet();
        }

        void visitInner() {
            innerNodeVisited.incrementAndGet();
        }
    }

    void consumeDebugInfo(DebugInfo debug) {
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

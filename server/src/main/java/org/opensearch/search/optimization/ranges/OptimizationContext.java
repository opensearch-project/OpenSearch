/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.optimization.ranges;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.function.BiConsumer;

import static org.opensearch.search.optimization.ranges.Helper.loggerName;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Context for doing the optimization for range-type aggregation
 */
public class OptimizationContext {

    private static final Logger logger = LogManager.getLogger(loggerName);

    private boolean rewriteable = false;
    private boolean rangesBuiltAtShardLevel = false;

    private final AggregatorDataProvider aggregatorDataProvider;
    private final SearchContext context;

    private MappedFieldType fieldType;
    private Ranges ranges;

    // debug info related fields
    public int leaf;
    public int inner;
    public int segments;
    public int optimizedSegments;

    public OptimizationContext(SearchContext context, AggregatorDataProvider aggregatorDataProvider) {
        this.context = context;
        this.aggregatorDataProvider = aggregatorDataProvider;
    }

    public boolean canOptimize(final Object parent, final int subAggLength) {
        if (context.maxAggRewriteFilters() == 0) return false;

        if (parent != null || subAggLength != 0) return false;

        boolean rewriteable = aggregatorDataProvider.canOptimize();
        logger.debug("Fast filter rewriteable: {} for shard {}", rewriteable, context.indexShard().shardId());
        this.rewriteable = rewriteable;
        if (rewriteable) {
            aggregatorDataProvider.setOptimizationContext(this);
        }
        return rewriteable;
    }

    public void buildRanges(MappedFieldType fieldType) throws IOException {
        assert ranges == null : "Ranges should only be built once at shard level, but they are already built";
        this.fieldType = fieldType;
        this.aggregatorDataProvider.buildRanges(context);
        if (ranges != null) {
            logger.debug("Ranges built for shard {}", context.indexShard().shardId());
            rangesBuiltAtShardLevel = true;
        }
    }

    private Ranges buildRanges(LeafReaderContext leaf) throws IOException {
        this.aggregatorDataProvider.buildRanges(leaf, context);
        if (ranges != null) {
            logger.debug("Ranges built for shard {} segment {}", context.indexShard().shardId(), leaf.ord);
        }
        return ranges;
    }

    Ranges getRanges() {
        return ranges;
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
     */
    public boolean tryFastFilterAggregation(final LeafReaderContext leafCtx, final BiConsumer<Long, Long> incrementDocCount)
        throws IOException {
        this.segments++;
        if (!this.rewriteable) {
            return false;
        }

        if (leafCtx.reader().hasDeletions()) return false;

        PointValues values = leafCtx.reader().getPointValues(this.fieldType.name());
        if (values == null) return false;
        // only proceed if every document corresponds to exactly one point
        if (values.getDocCount() != values.size()) return false;

        NumericDocValues docCountValues = DocValues.getNumeric(leafCtx.reader(), DocCountFieldMapper.NAME);
        if (docCountValues.nextDoc() != NO_MORE_DOCS) {
            logger.debug(
                "Shard {} segment {} has at least one document with _doc_count field, skip fast filter optimization",
                this.context.indexShard().shardId(),
                leafCtx.ord
            );
            return false;
        }

        // even if no ranges built at shard level, we can still perform the optimization
        // when functionally match-all at segment level
        if (!this.rangesBuiltAtShardLevel && !Helper.segmentMatchAll(this.context, leafCtx)) {
            return false;
        }

        Ranges ranges = this.ranges;
        if (ranges == null) {
            logger.debug(
                "Shard {} segment {} functionally match all documents. Build the fast filter",
                this.context.indexShard().shardId(),
                leafCtx.ord
            );
            ranges = this.buildRanges(leafCtx);
            if (ranges == null) {
                return false;
            }
        }

        this.aggregatorDataProvider.tryFastFilterAggregation(values, incrementDocCount);

        this.optimizedSegments++;
        logger.debug("Fast filter optimization applied to shard {} segment {}", this.context.indexShard().shardId(), leafCtx.ord);
        logger.debug("crossed leaf nodes: {}, inner nodes: {}", this.leaf, this.inner);
        return true;
    }

    void consumeDebugInfo(DebugInfo debug) {
        leaf += debug.leaf;
        inner += debug.inner;
    }

    /**
     * Internal ranges representation for the optimization
     */
    static class Ranges {
        byte[][] lowers; // inclusive
        byte[][] uppers; // exclusive
        int size;
        int byteLen;
        static ArrayUtil.ByteArrayComparator comparator;

        Ranges(byte[][] lowers, byte[][] uppers) {
            this.lowers = lowers;
            this.uppers = uppers;
            assert lowers.length == uppers.length;
            this.size = lowers.length;
            this.byteLen = lowers[0].length;
            comparator = ArrayUtil.getUnsignedComparator(byteLen);
        }

        public int firstRangeIndex(byte[] globalMin, byte[] globalMax) {
            if (compareByteValue(lowers[0], globalMax) > 0) {
                return -1;
            }
            int i = 0;
            while (compareByteValue(uppers[i], globalMin) <= 0) {
                i++;
                if (i >= size) {
                    return -1;
                }
            }
            return i;
        }

        public static int compareByteValue(byte[] value1, byte[] value2) {
            return comparator.compare(value1, 0, value2, 0);
        }

        public static boolean withinLowerBound(byte[] value, byte[] lowerBound) {
            return compareByteValue(value, lowerBound) >= 0;
        }

        public static boolean withinUpperBound(byte[] value, byte[] upperBound) {
            return compareByteValue(value, upperBound) < 0;
        }
    }

    private static class RangeCollectorForPointTree {
        private final BiConsumer<Integer, Integer> incrementRangeDocCount;
        private int counter = 0;

        private final Ranges ranges;
        private int activeIndex;

        private int visitedRange = 0;
        private final int maxNumNonZeroRange;

        public RangeCollectorForPointTree(
            BiConsumer<Integer, Integer> incrementRangeDocCount,
            int maxNumNonZeroRange,
            Ranges ranges,
            int activeIndex
        ) {
            this.incrementRangeDocCount = incrementRangeDocCount;
            this.maxNumNonZeroRange = maxNumNonZeroRange;
            this.ranges = ranges;
            this.activeIndex = activeIndex;
        }

        private void count() {
            counter++;
        }

        private void countNode(int count) {
            counter += count;
        }

        private void finalizePreviousRange() {
            if (counter > 0) {
                incrementRangeDocCount.accept(activeIndex, counter);
                counter = 0;
            }
        }

        /**
         * @return true when iterator exhausted or collect enough non-zero ranges
         */
        private boolean iterateRangeEnd(byte[] value) {
            // the new value may not be contiguous to the previous one
            // so try to find the first next range that cross the new value
            while (!withinUpperBound(value)) {
                if (++activeIndex >= ranges.size) {
                    return true;
                }
            }
            visitedRange++;
            return visitedRange > maxNumNonZeroRange;
        }

        private boolean withinLowerBound(byte[] value) {
            return Ranges.withinLowerBound(value, ranges.lowers[activeIndex]);
        }

        private boolean withinUpperBound(byte[] value) {
            return Ranges.withinUpperBound(value, ranges.uppers[activeIndex]);
        }

        private boolean withinRange(byte[] value) {
            return withinLowerBound(value) && withinUpperBound(value);
        }
    }

    /**
     * @param maxNumNonZeroRanges the number of non-zero ranges to collect
     */
    static OptimizationContext.DebugInfo multiRangesTraverse(
        final PointValues.PointTree tree,
        final OptimizationContext.Ranges ranges,
        final BiConsumer<Integer, Integer> incrementDocCount,
        final int maxNumNonZeroRanges
    ) throws IOException {
        OptimizationContext.DebugInfo debugInfo = new OptimizationContext.DebugInfo();
        int activeIndex = ranges.firstRangeIndex(tree.getMinPackedValue(), tree.getMaxPackedValue());
        if (activeIndex < 0) {
            logger.debug("No ranges match the query, skip the fast filter optimization");
            return debugInfo;
        }
        OptimizationContext.RangeCollectorForPointTree collector = new OptimizationContext.RangeCollectorForPointTree(
            incrementDocCount,
            maxNumNonZeroRanges,
            ranges,
            activeIndex
        );
        PointValues.IntersectVisitor visitor = getIntersectVisitor(collector);
        try {
            intersectWithRanges(visitor, tree, collector, debugInfo);
        } catch (CollectionTerminatedException e) {
            logger.debug("Early terminate since no more range to collect");
        }
        collector.finalizePreviousRange();

        return debugInfo;
    }

    private static void intersectWithRanges(
        PointValues.IntersectVisitor visitor,
        PointValues.PointTree pointTree,
        OptimizationContext.RangeCollectorForPointTree collector,
        OptimizationContext.DebugInfo debug
    ) throws IOException {
        PointValues.Relation r = visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());

        switch (r) {
            case CELL_INSIDE_QUERY:
                collector.countNode((int) pointTree.size());
                debug.visitInner();
                break;
            case CELL_CROSSES_QUERY:
                if (pointTree.moveToChild()) {
                    do {
                        intersectWithRanges(visitor, pointTree, collector, debug);
                    } while (pointTree.moveToSibling());
                    pointTree.moveToParent();
                } else {
                    pointTree.visitDocValues(visitor);
                    debug.visitLeaf();
                }
                break;
            case CELL_OUTSIDE_QUERY:
        }
    }

    private static PointValues.IntersectVisitor getIntersectVisitor(OptimizationContext.RangeCollectorForPointTree collector) {
        return new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) {
                // this branch should be unreachable
                throw new UnsupportedOperationException(
                    "This IntersectVisitor does not perform any actions on a " + "docID=" + docID + " node being visited"
                );
            }

            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
                visitPoints(packedValue, collector::count);
            }

            @Override
            public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                visitPoints(packedValue, () -> {
                    for (int doc = iterator.nextDoc(); doc != NO_MORE_DOCS; doc = iterator.nextDoc()) {
                        collector.count();
                    }
                });
            }

            private void visitPoints(byte[] packedValue, CheckedRunnable<IOException> collect) throws IOException {
                if (!collector.withinUpperBound(packedValue)) {
                    collector.finalizePreviousRange();
                    if (collector.iterateRangeEnd(packedValue)) {
                        throw new CollectionTerminatedException();
                    }
                }

                if (collector.withinRange(packedValue)) {
                    collect.run();
                }
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                // try to find the first range that may collect values from this cell
                if (!collector.withinUpperBound(minPackedValue)) {
                    collector.finalizePreviousRange();
                    if (collector.iterateRangeEnd(minPackedValue)) {
                        throw new CollectionTerminatedException();
                    }
                }
                // after the loop, min < upper
                // cell could be outside [min max] lower
                if (!collector.withinLowerBound(maxPackedValue)) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
                if (collector.withinRange(minPackedValue) && collector.withinRange(maxPackedValue)) {
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                }
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
        };
    }

    /**
     * Contains debug info of BKD traversal to show in profile
     */
    static class DebugInfo {
        private int leaf = 0; // leaf node visited
        private int inner = 0; // inner node visited

        private void visitLeaf() {
            leaf++;
        }

        private void visitInner() {
            inner++;
        }
    }
}

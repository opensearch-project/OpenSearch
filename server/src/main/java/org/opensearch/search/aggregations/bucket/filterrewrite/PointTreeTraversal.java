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
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.function.BiConsumer;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Utility class for traversing a {@link PointValues.PointTree} and collecting document counts for the ranges.
 *
 * <p>The main entry point is the {@link #multiRangesTraverse(RangeAwareIntersectVisitor)} method
 *
 * <p>The class uses a {@link RangeAwareIntersectVisitor} to keep track of the active ranges, traverse the tree, and
 * consume documents.
 */
public final class PointTreeTraversal {
    private static final Logger logger = LogManager.getLogger(Helper.loggerName);

    /**
     * Traverse the RangeAwareIntersectVisitor PointTree.
     * Collects and returns DebugInfo from traversal
     * @param visitor  the maximum number of non-zero ranges to collect
     * @return a {@link  FilterRewriteOptimizationContext.DebugInfo} object containing debug information about the traversal
     */
    public static FilterRewriteOptimizationContext.DebugInfo multiRangesTraverse(RangeAwareIntersectVisitor visitor) throws IOException {
        FilterRewriteOptimizationContext.DebugInfo debugInfo = new FilterRewriteOptimizationContext.DebugInfo();

        if (visitor.getActiveIndex() < 0) {
            logger.debug("No ranges match the query, skip the fast filter optimization");
            return debugInfo;
        }

        try {
            visitor.traverse(debugInfo);
        } catch (CollectionTerminatedException e) {
            logger.debug("Early terminate since no more range to collect");
        }

        return debugInfo;
    }

    /**
     * This IntersectVisitor contains a packed value representation of Ranges
     * as well as the current activeIndex being considered for collection.
     */
    public static abstract class RangeAwareIntersectVisitor implements PointValues.IntersectVisitor {
        private final PointValues.PointTree pointTree;
        private final PackedValueRanges packedValueRanges;
        private final int maxNumNonZeroRange;
        protected int visitedRange = 0;
        protected int activeIndex;

        public RangeAwareIntersectVisitor(PointValues.PointTree pointTree, PackedValueRanges packedValueRanges, int maxNumNonZeroRange) {
            this.packedValueRanges = packedValueRanges;
            this.pointTree = pointTree;
            this.maxNumNonZeroRange = maxNumNonZeroRange;
            this.activeIndex = packedValueRanges.firstRangeIndex(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
        }

        public long getActiveIndex() {
            return activeIndex;
        }

        public abstract void visit(int docID);

        public abstract void visit(int docID, byte[] packedValue);

        public abstract void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException;

        protected abstract void consumeContainedNode(PointValues.PointTree pointTree) throws IOException;

        protected abstract void consumeCrossedNode(PointValues.PointTree pointTree) throws IOException;

        public void traverse(FilterRewriteOptimizationContext.DebugInfo debug) throws IOException {
            PointValues.Relation r = compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
            switch (r) {
                case CELL_INSIDE_QUERY:
                    consumeContainedNode(pointTree);
                    debug.visitInner();
                    break;
                case CELL_CROSSES_QUERY:
                    if (pointTree.moveToChild()) {
                        do {
                            traverse(debug);
                        } while (pointTree.moveToSibling());
                        pointTree.moveToParent();
                    } else {
                        consumeCrossedNode(pointTree);
                        debug.visitLeaf();
                    }
                    break;
                case CELL_OUTSIDE_QUERY:
            }
        }

        /**
         * increment activeIndex until we run out of ranges or find a valid range that contains maxPackedValue
         * else throw CollectionTerminatedException if we run out of ranges to check
         * @param minPackedValue lower bound of PointValues.PointTree node
         * @param maxPackedValue upper bound of PointValues.PointTree node
         * @return the min/max values of the PointValues.PointTree node can be one of:
         * 1.) Completely outside the activeIndex range
         * 2.) Completely inside the activeIndex range
         * 3.) Overlapping with the activeIndex range
         */
        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            // try to find the first range that may collect values from this cell
            if (!packedValueRanges.withinUpperBound(minPackedValue, activeIndex) && iterateRangeEnd(minPackedValue)) {
                throw new CollectionTerminatedException();
            }

            // after the loop, min < upper
            // cell could be outside [min max] lower
            if (!packedValueRanges.withinLowerBound(maxPackedValue, activeIndex) && iterateRangeEnd(maxPackedValue)) {
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }

            if (packedValueRanges.withinRange(minPackedValue, activeIndex) && packedValueRanges.withinRange(maxPackedValue, activeIndex)) {
                return PointValues.Relation.CELL_INSIDE_QUERY;
            }
            return PointValues.Relation.CELL_CROSSES_QUERY;
        }

        /**
         * throws CollectionTerminatedException if we have reached our last range, and it does not contain packedValue
         * @param packedValue determine if packedValue falls within the range at activeIndex
         * @return true when packedValue falls within the activeIndex range
         */
        protected boolean canCollect(byte[] packedValue) {
            if (!packedValueRanges.withinUpperBound(packedValue, activeIndex) && iterateRangeEnd(packedValue)) {
                throw new CollectionTerminatedException();
            }
            return packedValueRanges.withinRange(packedValue, activeIndex);
        }

        /**
         * @param packedValue increment active index until we reach a range containing value
         * @return true when we've exhausted all available ranges or visited maxNumNonZeroRange and can stop early
         */
        protected boolean iterateRangeEnd(byte[] packedValue) {
            // the new value may not be contiguous to the previous one
            // so try to find the first next range that cross the new value
            while (!packedValueRanges.withinUpperBound(packedValue, activeIndex)) {
                if (++activeIndex >= packedValueRanges.size) {
                    return true;
                }
            }
            visitedRange++;
            return visitedRange > maxNumNonZeroRange;
        }
    }

    /**
     * Traverse PointTree with countDocs callback where countDock inputs are
     * 1.) activeIndex for range in which document(s) reside
     * 2.) total documents counted
     */
    public static class DocCountRangeAwareIntersectVisitor extends RangeAwareIntersectVisitor {
        BiConsumer<Integer, Integer> countDocs;

        public DocCountRangeAwareIntersectVisitor(
            PointValues.PointTree pointTree,
            PackedValueRanges packedValueRanges,
            int maxNumNonZeroRange,
            BiConsumer<Integer, Integer> countDocs
        ) {
            super(pointTree, packedValueRanges, maxNumNonZeroRange);
            this.countDocs = countDocs;
        }

        @Override
        public void visit(int docID) {
            countDocs.accept(activeIndex, 1);
        }

        @Override
        public void visit(int docID, byte[] packedValue) {
            if (canCollect(packedValue)) {
                countDocs.accept(activeIndex, 1);
            }
        }

        public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
            if (canCollect(packedValue)) {
                for (int doc = iterator.nextDoc(); doc != NO_MORE_DOCS; doc = iterator.nextDoc()) {
                    countDocs.accept(activeIndex, 1);
                }
            }
        }

        protected void consumeContainedNode(PointValues.PointTree pointTree) throws IOException {
            countDocs.accept(activeIndex, (int) pointTree.size());
        }

        protected void consumeCrossedNode(PointValues.PointTree pointTree) throws IOException {
            pointTree.visitDocValues(this);
        }
    }

    /**
     * Traverse PointTree with collectDocs callback where collectDocs inputs are
     * 1.) activeIndex for range in which document(s) reside
     * 2.) document id to collect
     */
    public static class DocCollectRangeAwareIntersectVisitor extends RangeAwareIntersectVisitor {
        BiConsumer<Integer, Integer> collectDocs;

        public DocCollectRangeAwareIntersectVisitor(
            PointValues.PointTree pointTree,
            PackedValueRanges packedValueRanges,
            int maxNumNonZeroRange,
            BiConsumer<Integer, Integer> collectDocs
        ) {
            super(pointTree, packedValueRanges, maxNumNonZeroRange);
            this.collectDocs = collectDocs;
        }

        @Override
        public void visit(int docID) {
            collectDocs.accept(activeIndex, docID);
        }

        @Override
        public void visit(int docID, byte[] packedValue) {
            if (canCollect(packedValue)) {
                collectDocs.accept(activeIndex, docID);
            }
        }

        public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
            if (canCollect(packedValue)) {
                for (int doc = iterator.nextDoc(); doc != NO_MORE_DOCS; doc = iterator.nextDoc()) {
                    collectDocs.accept(activeIndex, iterator.docID());
                }
            }
        }

        protected void consumeContainedNode(PointValues.PointTree pointTree) throws IOException {
            pointTree.visitDocIDs(this);
        }

        protected void consumeCrossedNode(PointValues.PointTree pointTree) throws IOException {
            pointTree.visitDocValues(this);
        }
    }
}

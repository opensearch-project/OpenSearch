/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.optimization.filterrewrite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import static org.opensearch.search.optimization.filterrewrite.Helper.loggerName;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Utility class for traversing a {@link PointValues.PointTree} and collecting document counts for the ranges.
 *
 * <p>The main entry point is the {@link #multiRangesTraverse(PointValues.PointTree, Ranges,
 * BiConsumer, int)} method
 *
 * <p>The class uses a {@link RangeCollectorForPointTree} to keep track of the active ranges and
 * determine which parts of the tree to visit. The {@link
 * PointValues.IntersectVisitor} implementation is responsible for the actual visitation and
 * document count collection.
 */
public final class TreeTraversal {
    private TreeTraversal() {}

    private static final Logger logger = LogManager.getLogger(loggerName);

    /**
     * Traverses the given {@link PointValues.PointTree} and collects document counts for the intersecting ranges.
     *
     * @param tree                 the point tree to traverse
     * @param ranges               the set of ranges to intersect with
     * @param collectRangeIDs      a callback to collect the doc ids for a range bucket
     * @param maxNumNonZeroRanges  the maximum number of non-zero ranges to collect
     * @return a {@link OptimizationContext.DebugInfo} object containing debug information about the traversal
     */
    public static OptimizationContext.DebugInfo multiRangesTraverse(
        final PointValues.PointTree tree,
        final Ranges ranges,
        final BiConsumer<Integer, List<Integer>> collectRangeIDs,
        final int maxNumNonZeroRanges
    ) throws IOException {
        OptimizationContext.DebugInfo debugInfo = new OptimizationContext.DebugInfo();
        int activeIndex = ranges.firstRangeIndex(tree.getMinPackedValue(), tree.getMaxPackedValue());
        if (activeIndex < 0) {
            logger.debug("No ranges match the query, skip the fast filter optimization");
            return debugInfo;
        }
        RangeCollectorForPointTree collector = new RangeCollectorForPointTree(collectRangeIDs, maxNumNonZeroRanges, ranges, activeIndex);
        PointValues.IntersectVisitor visitor = getIntersectVisitor(collector);
        try {
            intersectWithRanges(visitor, tree, debugInfo);
        } catch (CollectionTerminatedException e) {
            logger.debug("Early terminate since no more range to collect");
        }
        collector.finalizePreviousRange();

        return debugInfo;
    }

    private static void intersectWithRanges(
        PointValues.IntersectVisitor visitor,
        PointValues.PointTree pointTree,
        OptimizationContext.DebugInfo debug
    ) throws IOException {
        PointValues.Relation r = visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());

        switch (r) {
            case CELL_INSIDE_QUERY:
                pointTree.visitDocIDs(visitor);
                debug.visitInner();
                break;
            case CELL_CROSSES_QUERY:
                if (pointTree.moveToChild()) {
                    do {
                        intersectWithRanges(visitor, pointTree, debug);
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

    private static PointValues.IntersectVisitor getIntersectVisitor(RangeCollectorForPointTree collector) {
        return new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) {
                 collector.count(docID);
            }

            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
                if (canCollect(packedValue)) {
                    collector.count(docID);
                }
            }

            @Override
            public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                if (canCollect(packedValue)) {
                    for (int doc = iterator.nextDoc(); doc != NO_MORE_DOCS; doc = iterator.nextDoc()) {
                        collector.count(iterator.docID());
                    }
                }
            }

            private boolean canCollect(byte[] packedValue) {
                if (!collector.withinUpperBound(packedValue)) {
                    collector.finalizePreviousRange();
                    if (collector.iterateRangeEnd(packedValue)) {
                        throw new CollectionTerminatedException();
                    }
                }

                return collector.withinRange(packedValue);
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

    private static class RangeCollectorForPointTree {
        private final BiConsumer<Integer, List<Integer>> collectRangeIDs;
        private final List<Integer> DIDList = new ArrayList<>();
        private final Ranges ranges;
        private int activeIndex;
        private int visitedRange = 0;
        private final int maxNumNonZeroRange;

        public RangeCollectorForPointTree(
            BiConsumer<Integer, List<Integer>> collectRangeIDs,
            int maxNumNonZeroRange,
            Ranges ranges,
            int activeIndex
        ) {
            this.collectRangeIDs = collectRangeIDs;
            this.maxNumNonZeroRange = maxNumNonZeroRange;
            this.ranges = ranges;
            this.activeIndex = activeIndex;
        }

        private void count(int docID) {
            DIDList.add(docID);
        }

        private void countNode(List<Integer> docIDs) {
            DIDList.addAll(docIDs);
        }

        private void finalizePreviousRange() {
            if (!DIDList.isEmpty()) {
                collectRangeIDs.accept(activeIndex, DIDList);
                DIDList.clear();
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
}

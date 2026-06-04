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
import org.opensearch.common.CheckedRunnable;
import org.opensearch.search.aggregations.bucket.filterrewrite.rangecollector.RangeCollector;
import org.opensearch.search.aggregations.bucket.filterrewrite.rangecollector.SimpleRangeCollector;
import org.opensearch.search.aggregations.bucket.filterrewrite.rangecollector.SubAggRangeCollector;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Utility class for traversing a {@link PointValues.PointTree} and collecting document counts for the ranges.
 *
 * <p>The main entry point is the {@link #multiRangesTraverse} method
 *
 * <p>The class uses a {@link RangeCollector} to keep track of the active ranges and
 * determine which parts of the tree to visit. The {@link
 * PointValues.IntersectVisitor} implementation is responsible for the actual visitation and
 * document count collection.
 */
final class PointTreeTraversal {
    private PointTreeTraversal() {}

    private static final Logger logger = LogManager.getLogger(Helper.loggerName);

    /**
     * Creates an appropriate RangeCollector based on whether sub-aggregations are needed.
     */
    static RangeCollector createCollector(
        Ranges ranges,
        BiConsumer<Integer, Integer> incrementRangeDocCount,
        int maxNumNonZeroRange,
        int activeIndex,
        Function<Integer, Long> getBucketOrd,
        FilterRewriteOptimizationContext.OptimizeResult result,
        FilterRewriteOptimizationContext.SubAggCollectorParam subAggCollectorParam
    ) {
        if (subAggCollectorParam == null) {
            return new SimpleRangeCollector(ranges, incrementRangeDocCount, maxNumNonZeroRange, activeIndex, result);
        } else {
            return new SubAggRangeCollector(
                ranges,
                incrementRangeDocCount,
                maxNumNonZeroRange,
                activeIndex,
                result,
                getBucketOrd,
                subAggCollectorParam
            );
        }
    }

    /**
     * Traverses the given {@link PointValues.PointTree} and collects document counts for the intersecting ranges.
     *
     * @param tree      the point tree to traverse
     * @param collector the collector to use for gathering results
     * @return a {@link FilterRewriteOptimizationContext.OptimizeResult} object containing debug information about the traversal
     */
    static FilterRewriteOptimizationContext.OptimizeResult multiRangesTraverse(final PointValues.PointTree tree, RangeCollector collector)
        throws IOException {
        PointValues.IntersectVisitor visitor = getIntersectVisitor(collector);
        try {
            intersectWithRanges(visitor, tree, collector);
        } catch (CollectionTerminatedException e) {
            logger.debug("Early terminate since no more range to collect");
        }
        collector.finalizePreviousRange();
        return collector.getResult();
    }

    private static void intersectWithRanges(PointValues.IntersectVisitor visitor, PointValues.PointTree pointTree, RangeCollector collector)
        throws IOException {
        PointValues.Relation r = visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());

        switch (r) {
            case CELL_INSIDE_QUERY:
                if (collector.hasSubAgg()) {
                    // counter for top level agg is handled by sub agg collect
                    pointTree.visitDocIDs(visitor);
                } else {
                    // count node should be invoked only in absence of
                    // sub agg to not include the delete documents
                    collector.countNode((int) pointTree.size());
                    collector.visitInner();
                }
                break;
            case CELL_CROSSES_QUERY:
                if (pointTree.moveToChild()) {
                    do {
                        intersectWithRanges(visitor, pointTree, collector);
                    } while (pointTree.moveToSibling());
                    pointTree.moveToParent();
                } else {
                    pointTree.visitDocValues(visitor);
                    collector.visitLeaf();
                }
                break;
            case CELL_OUTSIDE_QUERY:
        }
    }

    private static PointValues.IntersectVisitor getIntersectVisitor(RangeCollector collector) {
        return new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) {
                collector.collectDocId(docID);
            }

            @Override
            public void visit(DocIdSetIterator iterator) throws IOException {
                collector.collectDocIdSet(iterator);
            }

            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
                visitPoints(packedValue, () -> {
                    if (collector.hasSubAgg()) {
                        collector.collectDocId(docID);
                    } else {
                        collector.count();
                    }
                });
            }

            @Override
            public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                visitPoints(packedValue, () -> {
                    // note: iterator can only iterate once
                    for (int doc = iterator.nextDoc(); doc != NO_MORE_DOCS; doc = iterator.nextDoc()) {
                        if (collector.hasSubAgg()) {
                            collector.collectDocId(doc);
                        } else {
                            collector.count();
                        }
                    }
                });
            }

            private void visitPoints(byte[] packedValue, CheckedRunnable<IOException> collect) throws IOException {
                if (!collector.withinUpperBound(packedValue)) {
                    collector.finalizePreviousRange();
                    if (collector.iterateRangeEnd(packedValue, true)) {
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
                    if (collector.iterateRangeEnd(minPackedValue, false)) {
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
}

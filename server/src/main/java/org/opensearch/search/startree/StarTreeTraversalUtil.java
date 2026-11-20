/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNodeType;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.StarTreeValuesIterator;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.MatchAllFilter;
import org.opensearch.search.startree.filter.StarTreeFilter;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Filter operator for star tree data structure.
 *
 *  @opensearch.experimental
 *  @opensearch.internal
 */
public class StarTreeTraversalUtil {
    private static final Logger logger = LogManager.getLogger(StarTreeTraversalUtil.class);

    /**
         *   First go over the star tree and try to match as many dimensions as possible
         *   For the remaining columns, use star-tree doc values to match them
     */
    public static FixedBitSet getStarTreeResult(StarTreeValues starTreeValues, StarTreeFilter starTreeFilter, SearchContext searchContext)
        throws IOException {

        // The request's filter is shared by concurrent search slices, which traverse other segments at the same time
        StarTreeFilter segmentFilter = starTreeFilter.forSegment(starTreeValues, searchContext);

        StarTreeResult starTreeResult = traverseStarTree(starTreeValues, segmentFilter);

        // No matches, return an empty FixedBitSet
        if (starTreeResult.matchedDocIds() == null) {
            return new FixedBitSet(0);
        }

        // Sized to the last matching entry, leaf ranges included
        FixedBitSet bitSet = starTreeResult.matchedDocIds();

        // Temporary FixedBitSet for filtering, allocated only if a remaining column needs it
        FixedBitSet tempBitSet = null;

        // Process remaining predicate columns to further filter the results
        for (String remainingPredicateColumn : starTreeResult.remainingPredicateColumns()) {
            logger.debug("remainingPredicateColumn : {}", remainingPredicateColumn);

            // Get the query value directly
            List<DimensionFilter> dimensionFilters = segmentFilter.getFiltersForDimension(remainingPredicateColumn);

            // Skip filtering if a MatchAllFilter is present for this dimension, since it implies all values match and no further filtering
            // is needed
            boolean isMatchAllFilterPresent = false;
            for (DimensionFilter dimensionFilter : dimensionFilters) {
                if (dimensionFilter instanceof MatchAllFilter) {
                    isMatchAllFilterPresent = true;
                    break;
                }
            }
            if (isMatchAllFilterPresent) continue;

            StarTreeValuesIterator valuesIterator = starTreeValues.getDimensionValuesIterator(
                dimensionFilters.getFirst().getMatchingDimension()
            );
            if (tempBitSet == null) {
                tempBitSet = new FixedBitSet(bitSet.length());
            } else {
                tempBitSet.clear();
            }

            // Iterate over the current set of matched document IDs
            for (int entryId = bitSet.nextSetBit(0); entryId != DocIdSetIterator.NO_MORE_DOCS; entryId = (entryId + 1 < bitSet.length())
                ? bitSet.nextSetBit(entryId + 1)
                : DocIdSetIterator.NO_MORE_DOCS) {
                if (valuesIterator.advanceExact(entryId)) {
                    long value = valuesIterator.value();
                    for (DimensionFilter dimensionFilter : dimensionFilters) {
                        if (dimensionFilter.matchDimValue(value, starTreeValues)) {
                            tempBitSet.set(entryId);// Set bit for the matching entryId
                            break;
                        }
                    }
                }
            }

            // Perform intersection of the current matches with the temp results for this predicate
            bitSet.and(tempBitSet);
        }

        return bitSet;  // Return the final FixedBitSet with all matches
    }

    /**
     * Helper method to traverse the star tree, get matching documents and keep track of all the
     * predicate dimensions that are not matched.
     */
    private static StarTreeResult traverseStarTree(StarTreeValues starTreeValues, StarTreeFilter starTreeFilter) throws IOException {
        MatchedRanges matched = new MatchedRanges();
        Set<String> globalRemainingPredicateColumns = null;
        StarTreeNode starTree = starTreeValues.getRoot();
        List<Dimension> dimensionsOrder = starTreeValues.getStarTreeField().getDimensionsOrder();
        List<String> dimensionNames = dimensionsOrder.stream().map(Dimension::getField).collect(Collectors.toList());
        boolean foundLeafNode = starTree.isLeaf();
        assert foundLeafNode == false; // root node is never leaf
        Queue<StarTreeNode> queue = new ArrayDeque<>();
        queue.add(starTree);
        int currentDimensionId = -1;
        Set<String> remainingPredicateColumns = new HashSet<>(starTreeFilter.getMatchingDimensions());
        StarTreeNode starTreeNode;

        while ((starTreeNode = queue.poll()) != null) {
            int dimensionId = starTreeNode.getDimensionId();
            if (dimensionId > currentDimensionId) {
                String dimension = dimensionNames.get(dimensionId);
                remainingPredicateColumns.remove(dimension);
                if (foundLeafNode && globalRemainingPredicateColumns == null) {
                    globalRemainingPredicateColumns = new HashSet<>(remainingPredicateColumns);
                }
                currentDimensionId = dimensionId;
            }

            if (remainingPredicateColumns.isEmpty()) {
                int docId = starTreeNode.getAggregatedDocId();
                matched.add(docId, docId + 1);
                continue;
            }

            if (starTreeNode.isLeaf()) {
                // A leaf's documents are a contiguous range: mark it at once
                int startDocId = starTreeNode.getStartDocId();
                int endDocId = starTreeNode.getEndDocId();
                if (endDocId > startDocId) {
                    matched.add(startDocId, endDocId);
                }
                continue;
            }

            String childDimension = dimensionNames.get(dimensionId + 1);
            StarTreeNode starNode = null;
            if (globalRemainingPredicateColumns == null || !globalRemainingPredicateColumns.contains(childDimension)) {
                starNode = starTreeNode.getChildStarNode();
            }

            if (remainingPredicateColumns.contains(childDimension)) {
                List<DimensionFilter> dimensionFilters = starTreeFilter.getFiltersForDimension(childDimension);
                final boolean[] tempFoundLeafNodes = new boolean[1];
                for (DimensionFilter dimensionFilter : dimensionFilters) {
                    dimensionFilter.matchStarTreeNodes(starTreeNode, starTreeValues, node -> {
                        queue.add(node);
                        tempFoundLeafNodes[0] |= node.isLeaf();
                    });
                }
                foundLeafNode |= tempFoundLeafNodes[0];
            } else {
                if (starNode != null) {
                    queue.add(starNode);
                    foundLeafNode |= starNode.isLeaf();
                } else {
                    Iterator<? extends StarTreeNode> childrenIterator = starTreeNode.getChildrenIterator();
                    while (childrenIterator.hasNext()) {
                        StarTreeNode childNode = childrenIterator.next();
                        if (childNode.getStarTreeNodeType() != StarTreeNodeType.STAR.getValue()) {
                            queue.add(childNode);
                            foundLeafNode |= childNode.isLeaf();
                        }
                    }
                }
            }
        }

        return new StarTreeResult(
            matched.toBitSet(),
            globalRemainingPredicateColumns != null ? globalRemainingPredicateColumns : Collections.emptySet()
        );
    }

    /**
     * Matching star-tree entries as [start, end) ranges: a leaf's documents, or one aggregated document. They are set
     * in a bitset sized to the last match once the traversal ends, so no result bitset is allocated for a segment
     * without matches.
     */
    private static final class MatchedRanges {
        private int[] bounds = new int[16];
        private int size;
        private int maxEnd;

        void add(int start, int end) {
            bounds = ArrayUtil.grow(bounds, size + 2);
            bounds[size++] = start;
            bounds[size++] = end;
            maxEnd = Math.max(maxEnd, end);
        }

        /** The matching entries, or null when nothing matched. */
        FixedBitSet toBitSet() {
            if (maxEnd == 0) {
                return null;
            }
            FixedBitSet bitSet = new FixedBitSet(maxEnd);
            for (int i = 0; i < size; i += 2) {
                bitSet.set(bounds[i], bounds[i + 1]);
            }
            return bitSet;
        }
    }

    /**
     * The result from traversing the star tree: the matching entries (null when nothing matched) and the predicate
     * dimensions the traversal did not match, to filter entry by entry.
     */
    private record StarTreeResult(FixedBitSet matchedDocIds, Set<String> remainingPredicateColumns) {
    }
}

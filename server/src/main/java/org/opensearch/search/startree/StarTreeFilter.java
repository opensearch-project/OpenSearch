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
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.StarTreeValuesIterator;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Filter operator for star tree data structure.
 *
 *  @opensearch.experimental
 *  @opensearch.internal
 */
public class StarTreeFilter {
    private static final Logger logger = LogManager.getLogger(StarTreeFilter.class);

    private final Map<String, Long> queryMap;
    private final StarTreeValues starTreeValues;

    public StarTreeFilter(StarTreeValues starTreeAggrStructure, Map<String, Long> predicateEvaluators) {
        // This filter operator does not support AND/OR/NOT operations as of now.
        starTreeValues = starTreeAggrStructure;
        queryMap = predicateEvaluators != null ? predicateEvaluators : Collections.emptyMap();
    }

    /**
     * <ul>
     *   <li>First go over the star tree and try to match as many dimensions as possible
     *   <li>For the remaining columns, use star-tree doc values to match them
     * </ul>
     */
    public StarTreeValuesIterator getStarTreeResult() throws IOException {
        StarTreeResult starTreeResult = traverseStarTree();
        List<StarTreeValuesIterator> andIterators = new ArrayList<>();
        andIterators.add(new StarTreeValuesIterator(starTreeResult._matchedDocIds.build().iterator()));
        StarTreeValuesIterator starTreeValuesIterator = andIterators.get(0);

        int length = 0;
        // No matches, return
        if (starTreeResult.maxMatchedDoc == -1) {
            return starTreeValuesIterator;
        }
        for (String remainingPredicateColumn : starTreeResult._remainingPredicateColumns) {
            logger.debug("remainingPredicateColumn : {}, maxMatchedDoc : {} ", remainingPredicateColumn, starTreeResult.maxMatchedDoc);
            DocIdSetBuilder builder = new DocIdSetBuilder(starTreeResult.maxMatchedDoc + 1);
            SortedNumericStarTreeValuesIterator ndv = (SortedNumericStarTreeValuesIterator) this.starTreeValues.getDimensionValuesIterator(
                remainingPredicateColumn
            );
            List<Integer> entryIds = new ArrayList<>();
            long queryValue = queryMap.get(remainingPredicateColumn); // Get the query value directly

            while (starTreeValuesIterator.nextEntry() != NO_MORE_DOCS) {
                int entryId = starTreeValuesIterator.entryId();
                if (ndv.advance(entryId) > 0) {
                    final int valuesCount = ndv.valuesCount();
                    for (int i = 0; i < valuesCount; i++) {
                        long value = ndv.nextValue();
                        // Directly compare value with queryValue
                        if (value == queryValue) {
                            entryIds.add(entryId);
                            break;
                        }
                    }
                }
            }
            DocIdSetBuilder.BulkAdder adder = builder.grow(entryIds.size());
            for (int entryId : entryIds) {
                adder.add(entryId);
            }
            length = entryIds.size();
            starTreeValuesIterator = new StarTreeValuesIterator(builder.build().iterator());
        }
        return starTreeValuesIterator;
    }

    /**
     * Helper method to traverse the star tree, get matching documents and keep track of all the
     * predicate dimensions that are not matched.
     */
    private StarTreeResult traverseStarTree() throws IOException {
        DocIdSetBuilder docsWithField = new DocIdSetBuilder(this.starTreeValues.getStarTreeDocumentCount());
        DocIdSetBuilder.BulkAdder adder;
        Set<String> globalRemainingPredicateColumns = null;
        StarTreeNode starTree = starTreeValues.getRoot();
        List<String> dimensionNames = starTreeValues.getStarTreeField()
            .getDimensionsOrder()
            .stream()
            .map(Dimension::getField)
            .collect(Collectors.toList());
        boolean foundLeafNode = starTree.isLeaf();
        Queue<StarTreeNode> queue = new ArrayDeque<>();
        queue.add(starTree);
        int currentDimensionId = -1;
        Set<String> remainingPredicateColumns = new HashSet<>(queryMap.keySet());
        if (foundLeafNode) {
            globalRemainingPredicateColumns = new HashSet<>(remainingPredicateColumns);
        }
        int matchedDocsCountInStarTree = 0;
        int maxDocNum = -1;
        StarTreeNode starTreeNode;
        List<Integer> docIds = new ArrayList<>();

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
                docIds.add(docId);
                matchedDocsCountInStarTree++;
                maxDocNum = Math.max(docId, maxDocNum);
                continue;
            }

            if (starTreeNode.isLeaf()) {
                for (long i = starTreeNode.getStartDocId(); i < starTreeNode.getEndDocId(); i++) {
                    docIds.add((int) i);
                    matchedDocsCountInStarTree++;
                    maxDocNum = Math.max((int) i, maxDocNum);
                }
                continue;
            }

            String childDimension = dimensionNames.get(dimensionId + 1);
            StarTreeNode starNode = null;
            if (globalRemainingPredicateColumns == null || !globalRemainingPredicateColumns.contains(childDimension)) {
                starNode = starTreeNode.getChildStarNode();
            }

            if (remainingPredicateColumns.contains(childDimension)) {
                long queryValue = queryMap.get(childDimension); // Get the query value directly from the map
                StarTreeNode matchingChild = starTreeNode.getChildForDimensionValue(queryValue);
                if (matchingChild != null) {
                    queue.add(matchingChild);
                    foundLeafNode |= matchingChild.isLeaf();
                }
            } else {
                if (starNode != null) {
                    queue.add(starNode);
                    foundLeafNode |= starNode.isLeaf();
                } else {
                    Iterator<? extends StarTreeNode> childrenIterator = starTreeNode.getChildrenIterator();
                    while (childrenIterator.hasNext()) {
                        StarTreeNode childNode = childrenIterator.next();
                        if (childNode.getDimensionValue() != StarTreeUtils.ALL) {
                            queue.add(childNode);
                            foundLeafNode |= childNode.isLeaf();
                        }
                    }
                }
            }
        }

        adder = docsWithField.grow(docIds.size());
        for (int id : docIds) {
            adder.add(id);
        }
        return new StarTreeResult(
            docsWithField,
            globalRemainingPredicateColumns != null ? globalRemainingPredicateColumns : Collections.emptySet(),
            matchedDocsCountInStarTree,
            maxDocNum
        );
    }

    /**
     * Helper class to wrap the result from traversing the star tree.
     * */
    static class StarTreeResult {
        final DocIdSetBuilder _matchedDocIds;
        final Set<String> _remainingPredicateColumns;
        final int numOfMatchedDocs;
        final int maxMatchedDoc;

        StarTreeResult(DocIdSetBuilder matchedDocIds, Set<String> remainingPredicateColumns, int numOfMatchedDocs, int maxMatchedDoc) {
            _matchedDocIds = matchedDocIds;
            _remainingPredicateColumns = remainingPredicateColumns;
            this.numOfMatchedDocs = numOfMatchedDocs;
            this.maxMatchedDoc = maxMatchedDoc;
        }
    }
}

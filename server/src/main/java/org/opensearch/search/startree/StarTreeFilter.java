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
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils;

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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Filter operator for star tree data structure.
 *
 *  @opensearch.experimental
 */
public class StarTreeFilter {
    private static final Logger logger = LogManager.getLogger(StarTreeFilter.class);

    private final StarTreeNode starTreeRoot;
    Map<String, List<Range>> _predicateEvaluators;
    DocIdSetBuilder docsWithField;
    Map<String, DocIdSetIterator> dimValueMap;

    public StarTreeFilter(StarTreeValues starTreeAggrStructure, Map<String, List<Range>> predicateEvaluators) {
        starTreeRoot = starTreeAggrStructure.getRoot();
        dimValueMap = starTreeAggrStructure.getDimensionDocValuesIteratorMap();
        _predicateEvaluators = predicateEvaluators != null ? predicateEvaluators : Collections.emptyMap();

        // TODO : this should be the maximum number of doc values
        docsWithField = new DocIdSetBuilder(Integer.MAX_VALUE);
    }

    /**
     * <ul>
     *   <li>First go over the star tree and try to match as many dimensions as possible
     *   <li>For the remaining columns, use doc values indexes to match them
     * </ul>
     */
    public DocIdSetIterator getStarTreeResult() throws IOException {
        StarTreeResult starTreeResult = traverseStarTree();
        List<DocIdSetIterator> andIterators = new ArrayList<>();
        andIterators.add(starTreeResult._matchedDocIds.build().iterator());
        DocIdSetIterator docIdSetIterator = andIterators.get(0);

        // No matches, return
        if (starTreeResult.maxMatchedDoc == -1) {
            return docIdSetIterator;
        }

        for (String remainingPredicateColumn : starTreeResult._remainingPredicateColumns) {
            logger.debug("remainingPredicateColumn : {}, maxMatchedDoc : {} ", remainingPredicateColumn, starTreeResult.maxMatchedDoc);
            DocIdSetBuilder builder = new DocIdSetBuilder(starTreeResult.maxMatchedDoc + 1);
            SortedNumericDocValues ndv = (SortedNumericDocValues) this.dimValueMap.get(remainingPredicateColumn);
            List<Integer> docIds = new ArrayList<>();
            List<Range> queryRanges = _predicateEvaluators.get(remainingPredicateColumn); // Get the list of min-max ranges for this field

            while (docIdSetIterator.nextDoc() != NO_MORE_DOCS) {
                int docID = docIdSetIterator.docID();
                if (ndv.advanceExact(docID)) {
                    final int valuesCount = ndv.docValueCount();
                    for (int i = 0; i < valuesCount; i++) {
                        double value = ndv.nextValue();
                        // Check if the value falls within any of the ranges in the query
                        boolean matchFound = false;
                        for (Range range : queryRanges) {
                            if (value >= range.getMin() && value <= range.getMax()) {
                                docIds.add(docID);
                                matchFound = true;
                                break;
                            }
                        }
                        if (matchFound) {
                            break;
                        }
                    }
                }
            }

            DocIdSetBuilder.BulkAdder adder = builder.grow(docIds.size());
            for (int docID : docIds) {
                adder.add(docID);
            }
            docIdSetIterator = builder.build().iterator();
        }
        return docIdSetIterator;
    }

    /**
     * Helper method to traverse the star tree, get matching documents and keep track of all the
     * predicate dimensions that are not matched.
     */
    private StarTreeResult traverseStarTree() throws IOException {
        Queue<StarTreeNode> queue = new ArrayDeque<>();
        queue.add(starTreeRoot);
        Set<String> globalRemainingPredicateColumns = null;

        int matchedDocsCountInStarTree = 0;
        int maxDocNum = -1;
        List<Integer> docIds = new ArrayList<>();
        boolean foundLeafNode = false;
        int currentDimensionId = -1;
        Set<String> remainingPredicateColumns = new HashSet<>(_predicateEvaluators.keySet());
        List<String> dimensionNames = new ArrayList<>(dimValueMap.keySet());

        while (!queue.isEmpty()) {
            StarTreeNode starTreeNode = queue.poll();
            int dimensionId = starTreeNode.getDimensionId();
            if (dimensionId > currentDimensionId) {
                String dimension = dimensionNames.get(dimensionId);
                remainingPredicateColumns.remove(dimension);
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
            if (remainingPredicateColumns.contains(childDimension)) {
                List<Range> queryRanges = _predicateEvaluators.get(childDimension);

                for (Range range : queryRanges) {
                    Iterator rangeIterator = starTreeNode.getChildrenIteratorForRange((int) range.getMin(), (int) range.getMax());

                    while (rangeIterator.hasNext()) {
                        StarTreeNode matchingChildNode = (StarTreeNode) rangeIterator.next();
                        queue.add(matchingChildNode);
                        foundLeafNode |= matchingChildNode.isLeaf();
                    }
                }
            } else {
                queue.add(starTreeNode.getChildForDimensionValue(StarTreeUtils.ALL, true));
            }
        }

        DocIdSetBuilder.BulkAdder adder = docsWithField.grow(docIds.size());
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
    private static class StarTreeResult {
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

    public static class Range {
        long min;
        long max;

        public Range(long min, long max) {
            this.min = min;
            this.max = max;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }
    }
}

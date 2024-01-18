/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.index.codec.freshstartree.query;

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
import java.util.function.Predicate;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.opensearch.index.codec.freshstartree.codec.StarTreeAggregatedValues;
import org.opensearch.index.codec.freshstartree.node.StarTree;
import org.opensearch.index.codec.freshstartree.node.StarTreeNode;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;


/** Filter operator for star tree data structure. */
public class StarTreeFilter {

    /** Helper class to wrap the result from traversing the star tree. */
    static class StarTreeResult {
        final DocIdSetBuilder _matchedDocIds;
        final Set<String> _remainingPredicateColumns;

        StarTreeResult(DocIdSetBuilder matchedDocIds, Set<String> remainingPredicateColumns) {
            _matchedDocIds = matchedDocIds;
            _remainingPredicateColumns = remainingPredicateColumns;
        }
    }

    private final StarTree _starTree;

    Map<String, List<Predicate<Long>>> _predicateEvaluators;
    private final Set<String> _groupByColumns;

    DocIdSetBuilder docsWithField;
    DocIdSetBuilder.BulkAdder adder;
    Map<String, NumericDocValues> dimValueMap;

    public StarTreeFilter(StarTreeAggregatedValues starTreeAggrStructure,
        Map<String, List<Predicate<Long>>> predicateEvaluators, Set<String> groupByColumns)
        throws IOException {
        // This filter operator does not support AND/OR/NOT operations.
        _starTree = starTreeAggrStructure._starTree;
        dimValueMap = starTreeAggrStructure.dimensionValues;
        _predicateEvaluators = predicateEvaluators != null ? predicateEvaluators : Collections.emptyMap();
        _groupByColumns = groupByColumns != null ? groupByColumns : Collections.emptySet();

        // TODO : this should be the maximum number of doc values
        docsWithField = new DocIdSetBuilder(Integer.MAX_VALUE);
    }

    /**
     * Helper method to get a filter operator that match the matchingDictIdsMap.
     *
     * <ul>
     *   <li>First go over the star tree and try to match as many columns as possible
     *   <li>For the remaining columns, use other indexes to match them
     * </ul>
     */
    public DocIdSetIterator getStarTreeResult()
        throws IOException {
        StarTreeResult starTreeResult = traverseStarTree();
        List<DocIdSetIterator> andIterators = new ArrayList<>();
        andIterators.add(starTreeResult._matchedDocIds.build().iterator());

        // System.out.println("Remaining predicate columns : " +
        // starTreeResult._remainingPredicateColumns.toString());
        for (String remainingPredicateColumn : starTreeResult._remainingPredicateColumns) {
            // TODO : set to max value of doc values
            DocIdSetBuilder builder = new DocIdSetBuilder(Integer.MAX_VALUE);
            List<Predicate<Long>> compositePredicateEvaluators = _predicateEvaluators.get(remainingPredicateColumn);
            NumericDocValues ndv = this.dimValueMap.get(remainingPredicateColumn);
            for (int docID = ndv.nextDoc(); docID != NO_MORE_DOCS; docID = ndv.nextDoc()) {
                for (Predicate<Long> compositePredicateEvaluator : compositePredicateEvaluators) {
                    // TODO : this might be expensive as its done against all doc values docs
                    if (compositePredicateEvaluator.test(ndv.longValue())) {
                        // System.out.println("Adding doc id : " + docID + " for status " + ndv.longValue());
                        builder.grow(1).add(docID);
                        break;
                    }
                }
            }
            andIterators.add(builder.build().iterator());
        }
        if (andIterators.size() > 1) {
            return ConjunctionUtils.intersectIterators(andIterators);
        }
        return andIterators.get(0);
    }

    /**
     * Helper method to traverse the star tree, get matching documents and keep track of all the
     * predicate columns that are not matched. Returns {@code null} if no matching dictionary id found
     * for a column (i.e. the result for the filter operator is empty).
     */
    private StarTreeResult traverseStarTree()
        throws IOException {
        Set<String> globalRemainingPredicateColumns = null;

        StarTree starTree = _starTree;
        List<String> dimensionNames = starTree.getDimensionNames();
        StarTreeNode starTreeRootNode = starTree.getRoot();

        // Track whether we have found a leaf node added to the queue. If we have found a leaf node, and
        // traversed to the
        // level of the leave node, we can set globalRemainingPredicateColumns if not already set
        // because we know the leaf
        // node won't split further on other predicate columns.
        boolean foundLeafNode = starTreeRootNode.isLeaf();

        // Use BFS to traverse the star tree
        Queue<StarTreeNode> queue = new ArrayDeque<>();
        queue.add(starTreeRootNode);
        int currentDimensionId = -1;
        Set<String> remainingPredicateColumns = new HashSet<>(_predicateEvaluators.keySet());
        Set<String> remainingGroupByColumns = new HashSet<>(_groupByColumns);
        if (foundLeafNode) {
            globalRemainingPredicateColumns = new HashSet<>(remainingPredicateColumns);
        }

        StarTreeNode starTreeNode;
        while ((starTreeNode = queue.poll()) != null) {
            int dimensionId = starTreeNode.getDimensionId();
            if (dimensionId > currentDimensionId) {
                // Previous level finished
                String dimension = dimensionNames.get(dimensionId);
                remainingPredicateColumns.remove(dimension);
                remainingGroupByColumns.remove(dimension);
                if (foundLeafNode && globalRemainingPredicateColumns == null) {
                    globalRemainingPredicateColumns = new HashSet<>(remainingPredicateColumns);
                }
                currentDimensionId = dimensionId;
            }

            // If all predicate columns and group-by columns are matched, we can use aggregated document
            if (remainingPredicateColumns.isEmpty() && remainingGroupByColumns.isEmpty()) {
                // System.out.println("Adding doc id for : " + dimensionNames.get(dimensionId) + " = " +
                // starTreeNode
                // .getAggregatedDocId());
                adder = docsWithField.grow(1);
                adder.add(starTreeNode.getAggregatedDocId());
                continue;
            }

            // For leaf node, because we haven't exhausted all predicate columns and group-by columns, we
            // cannot use
            // the aggregated document. Add the range of documents for this node to the bitmap, and keep
            // track of the
            // remaining predicate columns for this node
            if (starTreeNode.isLeaf()) {
                for (long i = starTreeNode.getStartDocId(); i < starTreeNode.getEndDocId(); i++) {
                    adder = docsWithField.grow(1);
                    // System.out.println("Adding doc id for : " + dimensionNames.get(dimensionId) + " = " +
                    // i);
                    adder.add((int) i);
                }
                continue;
            }

            // For non-leaf node, proceed to next level
            String childDimension = dimensionNames.get(dimensionId + 1);

            // Only read star-node when the dimension is not in the global remaining predicate columns or
            // group-by columns
            // because we cannot use star-node in such cases
            StarTreeNode starNode = null;
            if ((globalRemainingPredicateColumns == null || !globalRemainingPredicateColumns.contains(childDimension))
                && !remainingGroupByColumns.contains(childDimension)) {
                starNode = starTreeNode.getChildForDimensionValue(StarTreeNode.ALL);
            }

            if (remainingPredicateColumns.contains(childDimension)) {
                // Have predicates on the next level, add matching nodes to the queue

                // Calculate the matching dictionary ids for the child dimension
                int numChildren = starTreeNode.getNumChildren();

                // If number of matching dictionary ids is large, use scan instead of binary search

                Iterator<? extends StarTreeNode> childrenIterator = starTreeNode.getChildrenIterator();

                // When the star-node exists, and the number of matching doc ids is more than or equal to
                // the
                // number of non-star child nodes, check if all the child nodes match the predicate, and use
                // the
                // star-node if so
                if (starNode != null) {
                    List<StarTreeNode> matchingChildNodes = new ArrayList<>();
                    boolean findLeafChildNode = false;
                    while (childrenIterator.hasNext()) {
                        StarTreeNode childNode = childrenIterator.next();
                        List<Predicate<Long>> predicates = _predicateEvaluators.get(childDimension);
                        for (Predicate<Long> predicate : predicates) {
                            long val = childNode.getDimensionValue();
                            if (predicate.test(val)) {
                                matchingChildNodes.add(childNode);
                                findLeafChildNode |= childNode.isLeaf();
                                break;
                            }
                        }
                    }
                    if (matchingChildNodes.size() == numChildren - 1) {
                        // All the child nodes (except for the star-node) match the predicate, use the star-node
                        queue.add(starNode);
                        foundLeafNode |= starNode.isLeaf();
                    } else {
                        // Some child nodes do not match the predicate, use the matching child nodes
                        queue.addAll(matchingChildNodes);
                        foundLeafNode |= findLeafChildNode;
                    }
                } else {
                    // Cannot use the star-node, use the matching child nodes
                    while (childrenIterator.hasNext()) {
                        StarTreeNode childNode = childrenIterator.next();
                        List<Predicate<Long>> predicates = _predicateEvaluators.get(childDimension);
                        for (Predicate<Long> predicate : predicates) {
                            if (predicate.test(childNode.getDimensionValue())) {
                                queue.add(childNode);
                                foundLeafNode |= childNode.isLeaf();
                                break;
                            }
                        }
                    }
                }
            } else {
                // No predicate on the next level

                if (starNode != null) {
                    // Star-node exists, use it
                    queue.add(starNode);
                    foundLeafNode |= starNode.isLeaf();
                } else {
                    // Star-node does not exist or cannot be used, add all non-star nodes to the queue
                    Iterator<? extends StarTreeNode> childrenIterator = starTreeNode.getChildrenIterator();
                    while (childrenIterator.hasNext()) {
                        StarTreeNode childNode = childrenIterator.next();
                        if (childNode.getDimensionValue() != StarTreeNode.ALL) {
                            queue.add(childNode);
                            foundLeafNode |= childNode.isLeaf();
                        }
                    }
                }
            }
        }

        return new StarTreeResult(docsWithField,
            globalRemainingPredicateColumns != null ? globalRemainingPredicateColumns : Collections.emptySet());
    }
}

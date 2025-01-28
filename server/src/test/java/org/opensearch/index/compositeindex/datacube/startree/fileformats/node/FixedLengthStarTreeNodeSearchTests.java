/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.fileformats.node;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.StarTreeWriter;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.StarTreeMetadata;
import org.opensearch.index.compositeindex.datacube.startree.node.InMemoryTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeFactory;
import org.opensearch.search.aggregations.startree.ArrayBasedCollector;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.ALL;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FixedLengthStarTreeNodeSearchTests extends OpenSearchTestCase {

    public void testExactMatch() {
        long[] randomSorted = random().longs(100, Long.MIN_VALUE, Long.MAX_VALUE).toArray();
        Arrays.sort(randomSorted);
        for (boolean createStarNode : new boolean[] { true, false }) {
            for (boolean createNullNode : new boolean[] { true, false }) {
                createStarTreeForDimension(new long[] { -1, 1, 2, 5 }, createStarNode, createNullNode, List.of(fixedLengthStarTreeNode -> {
                    try {
                        boolean result = true;
                        FixedLengthStarTreeNode lastMatchedNode;
                        lastMatchedNode = (FixedLengthStarTreeNode) fixedLengthStarTreeNode.getChildForDimensionValue(-1L);
                        result &= -1 == lastMatchedNode.getDimensionValue();
                        // Leaf Node should return null
                        result &= null == lastMatchedNode.getChildForDimensionValue(5L);
                        result &= null == lastMatchedNode.getChildForDimensionValue(5L, lastMatchedNode);
                        // Asserting Last Matched Node works as expected
                        lastMatchedNode = (FixedLengthStarTreeNode) fixedLengthStarTreeNode.getChildForDimensionValue(1L, lastMatchedNode);
                        result &= 1 == lastMatchedNode.getDimensionValue();
                        lastMatchedNode = (FixedLengthStarTreeNode) fixedLengthStarTreeNode.getChildForDimensionValue(5L, lastMatchedNode);
                        result &= 5 == lastMatchedNode.getDimensionValue();
                        // Asserting null is returned when last matched node is after the value to search.
                        lastMatchedNode = (FixedLengthStarTreeNode) fixedLengthStarTreeNode.getChildForDimensionValue(2L, lastMatchedNode);
                        result &= null == lastMatchedNode;
                        // When dimension value is null
                        result &= null == fixedLengthStarTreeNode.getChildForDimensionValue(null);
                        result &= null == fixedLengthStarTreeNode.getChildForDimensionValue(null, null);
                        // non-existing dimensionValue
                        result &= null == fixedLengthStarTreeNode.getChildForDimensionValue(4L);
                        result &= null == fixedLengthStarTreeNode.getChildForDimensionValue(randomLongBetween(6, Long.MAX_VALUE));
                        result &= null == fixedLengthStarTreeNode.getChildForDimensionValue(randomLongBetween(Long.MIN_VALUE, -2));
                        return result;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }));
                createStarTreeForDimension(new long[] { 1 }, createStarNode, createNullNode, List.of(fixedLengthStarTreeNode -> {
                    try {
                        boolean result = true;
                        result &= 1 == fixedLengthStarTreeNode.getChildForDimensionValue(1L).getDimensionValue();
                        result &= null == fixedLengthStarTreeNode.getChildForDimensionValue(2L);
                        result &= null == fixedLengthStarTreeNode.getChildForDimensionValue(randomLongBetween(2, Long.MAX_VALUE));
                        result &= null == fixedLengthStarTreeNode.getChildForDimensionValue(randomLongBetween(Long.MIN_VALUE, 0));
                        return result;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }));
                createStarTreeForDimension(new long[] {}, createStarNode, createNullNode, List.of(fixedLengthStarTreeNode -> {
                    try {
                        boolean result = true;
                        result &= null == fixedLengthStarTreeNode.getChildForDimensionValue(1L);
                        result &= null == fixedLengthStarTreeNode.getChildForDimensionValue(randomLongBetween(0, Long.MAX_VALUE));
                        result &= null == fixedLengthStarTreeNode.getChildForDimensionValue(randomLongBetween(Long.MIN_VALUE, 0));
                        return result;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }));
                createStarTreeForDimension(randomSorted, createStarNode, createNullNode, List.of(fixedLengthStarTreeNode -> {
                    boolean result = true;
                    for (int i = 1; i <= 100; i++) {
                        try {
                            ArrayBasedCollector collector = new ArrayBasedCollector();
                            long key = randomLong();
                            FixedLengthStarTreeNode node = (FixedLengthStarTreeNode) fixedLengthStarTreeNode.getChildForDimensionValue(key);
                            long match = Arrays.binarySearch(randomSorted, key);
                            if (match >= 0) {
                                assertNotNull(node);
                                assertEquals(key, node.getDimensionValue());
                            } else {
                                assertEquals(0, collector.collectedNodeCount());
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return result;
                }));
            }
        }
    }

    public void testRangeMatch() {
        long[] randomSorted = random().longs(100, Long.MIN_VALUE, Long.MAX_VALUE).toArray();
        Arrays.sort(randomSorted);
        for (boolean createStarNode : new boolean[] { true, false }) {
            for (boolean createNullNode : new boolean[] { true, false }) {
                createStarTreeForDimension(
                    new long[] { -10, -1, 1, 2, 5, 9, 25 },
                    createStarNode,
                    createNullNode,
                    List.of(fixedLengthStarTreeNode -> {
                        try {
                            boolean result = true;
                            ArrayBasedCollector collector;
                            // Whole range
                            collector = new ArrayBasedCollector();
                            fixedLengthStarTreeNode.collectChildrenInRange(-20, 26, collector);
                            result &= collector.matchAllCollectedValues(new long[] { -10, -1, 1, 2, 5, 9, 25 });
                            // Subset matched from left
                            collector = new ArrayBasedCollector();
                            fixedLengthStarTreeNode.collectChildrenInRange(-2, 1, collector);
                            result &= collector.matchAllCollectedValues(new long[] { -1, 1 });
                            // Subset matched from right
                            collector = new ArrayBasedCollector();
                            fixedLengthStarTreeNode.collectChildrenInRange(6, 100, collector);
                            result &= collector.matchAllCollectedValues(new long[] { 9, 25 });
                            // No match on left
                            collector = new ArrayBasedCollector();
                            fixedLengthStarTreeNode.collectChildrenInRange(-30, -20, collector);
                            result &= collector.collectedNodeCount() == 0;
                            // No match on right
                            collector = new ArrayBasedCollector();
                            fixedLengthStarTreeNode.collectChildrenInRange(30, 50, collector);
                            result &= collector.collectedNodeCount() == 0;
                            // Low > High
                            collector = new ArrayBasedCollector();
                            fixedLengthStarTreeNode.collectChildrenInRange(50, 10, collector);
                            result &= collector.collectedNodeCount() == 0;
                            // Match leftmost
                            collector = new ArrayBasedCollector();
                            fixedLengthStarTreeNode.collectChildrenInRange(-30, -10, collector);
                            result &= collector.matchAllCollectedValues(new long[] { -10 });
                            // Match rightmost
                            collector = new ArrayBasedCollector();
                            fixedLengthStarTreeNode.collectChildrenInRange(10, 25, collector);
                            result &= collector.matchAllCollectedValues(new long[] { 25 });
                            // Match contains interval which has nothing
                            collector = new ArrayBasedCollector();
                            fixedLengthStarTreeNode.collectChildrenInRange(10, 24, collector);
                            result &= collector.collectedNodeCount() == 0;
                            // Match contains interval which has nothing
                            collector = new ArrayBasedCollector();
                            fixedLengthStarTreeNode.collectChildrenInRange(6, 24, collector);
                            result &= collector.matchAllCollectedValues(new long[] { 9 });
                            return result;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                );
                createStarTreeForDimension(randomSorted, createStarNode, createNullNode, List.of(fixedLengthStarTreeNode -> {
                    boolean result = true;
                    TreeSet<Long> treeSet = Arrays.stream(randomSorted).boxed().collect(Collectors.toCollection(TreeSet::new));
                    for (int i = 1; i <= 100; i++) {
                        try {
                            ArrayBasedCollector collector = new ArrayBasedCollector();
                            long low = randomLong(), high = randomLong();
                            fixedLengthStarTreeNode.collectChildrenInRange(low, high, collector);
                            if (low < high) {
                                Long lowValue = treeSet.ceiling(low);
                                if (lowValue != null) {
                                    Long highValue = treeSet.floor(high);
                                    if (highValue != null && highValue >= lowValue) {
                                        collector.matchAllCollectedValues(
                                            Arrays.copyOfRange(
                                                randomSorted,
                                                Arrays.binarySearch(randomSorted, lowValue),
                                                Arrays.binarySearch(randomSorted, highValue)
                                            )
                                        );
                                    } else if (lowValue <= high) {
                                        collector.matchAllCollectedValues(new long[] { lowValue });
                                    } else {
                                        assertEquals(0, collector.collectedNodeCount());
                                    }
                                } else {
                                    assertEquals(0, collector.collectedNodeCount());
                                }
                            } else if (low == high) {
                                collector.matchAllCollectedValues(new long[] { low });
                            } else {
                                assertEquals(0, collector.collectedNodeCount());
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return result;
                }));
            }
        }
    }

    private void createStarTreeForDimension(
        long[] dimensionValues,
        boolean createStarNode,
        boolean createNullNode,
        List<Predicate<FixedLengthStarTreeNode>> predicates
    ) {

        try (Directory directory = newFSDirectory(createTempDir())) {

            long starTreeDataLength;

            try (IndexOutput dataOut = directory.createOutput("star-tree-data", IOContext.DEFAULT)) {
                StarTreeWriter starTreeWriter = new StarTreeWriter();
                int starNodeLengthContribution = 0;

                InMemoryTreeNode rootNode = new InMemoryTreeNode(
                    0,
                    randomInt(),
                    randomInt(),
                    randomFrom((byte) 0, (byte) -1, (byte) 1),
                    -1
                );
                rootNode.setChildDimensionId(1);
                rootNode.setAggregatedDocId(randomInt());

                if (createStarNode && dimensionValues.length > 1) {
                    InMemoryTreeNode starChild = new InMemoryTreeNode(
                        rootNode.getDimensionId() + 1,
                        randomInt(),
                        randomInt(),
                        (byte) -1,
                        -1
                    );
                    starChild.setChildDimensionId(-1);
                    starChild.setAggregatedDocId(randomInt());
                    rootNode.addChildNode(starChild, (long) ALL);
                    starNodeLengthContribution++;
                }

                for (long dimensionValue : dimensionValues) {
                    InMemoryTreeNode defaultNode = new InMemoryTreeNode(
                        rootNode.getDimensionId() + 1,
                        randomInt(),
                        randomInt(),
                        (byte) 0,
                        dimensionValue
                    );
                    defaultNode.setChildDimensionId(-1);
                    defaultNode.setAggregatedDocId(randomInt());
                    rootNode.addChildNode(defaultNode, dimensionValue);
                }

                if (createNullNode) {
                    InMemoryTreeNode nullNode = new InMemoryTreeNode(rootNode.getDimensionId() + 1, randomInt(), randomInt(), (byte) 1, -1);
                    nullNode.setChildDimensionId(-1);
                    nullNode.setAggregatedDocId(randomInt());
                    rootNode.addChildNode(nullNode, null);
                }

                starTreeDataLength = starTreeWriter.writeStarTree(
                    dataOut,
                    rootNode,
                    starNodeLengthContribution + rootNode.getChildren().size() + 1,
                    "star-tree"
                );

                // asserting on the actual length of the star tree data file
                assertEquals(starTreeDataLength, (33L * rootNode.getChildren().size()) + (starNodeLengthContribution * 33L) + 33L);
            }

            for (Predicate<FixedLengthStarTreeNode> predicate : predicates) {
                try (IndexInput dataIn = directory.openInput("star-tree-data", IOContext.READONCE)) {
                    StarTreeMetadata starTreeMetadata = mock(StarTreeMetadata.class);
                    when(starTreeMetadata.getDataLength()).thenReturn(starTreeDataLength);
                    when(starTreeMetadata.getDataStartFilePointer()).thenReturn(0L);
                    FixedLengthStarTreeNode effectiveRoot = (FixedLengthStarTreeNode) StarTreeFactory.createStarTree(
                        dataIn,
                        starTreeMetadata
                    );
                    assertTrue(predicate.test(effectiveRoot));
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    // private static class ArrayBasedCollector implements StarTreeNodeCollector {
    //
    // private final List<StarTreeNode> nodes = new ArrayList<>();
    //
    // @Override
    // public void collectStarTreeNode(StarTreeNode node) {
    // nodes.add(node);
    // }
    //
    // public boolean matchAllCollectedValues(long[] values) throws IOException {
    // boolean matches = true;
    // for (int i = 0; i < values.length; i++) {
    // matches &= nodes.get(i).getDimensionValue() == values[i];
    // }
    // return matches;
    // }
    //
    // public int collectedNodeCount() {
    // return nodes.size();
    // }
    //
    // }

}

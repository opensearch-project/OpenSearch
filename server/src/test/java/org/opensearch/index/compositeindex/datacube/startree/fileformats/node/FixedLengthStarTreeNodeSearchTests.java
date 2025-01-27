/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.fileformats.node;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.StarTreeWriter;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.StarTreeMetadata;
import org.opensearch.index.compositeindex.datacube.startree.node.InMemoryTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeFactory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.ALL;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FixedLengthStarTreeNodeSearchTests extends OpenSearchTestCase {

    public void testExactMatch() {
        for (boolean createStarNode : new boolean[] { true, false }) {
            for (boolean createNullNode : new boolean[] { true, false }) {
                createStarTreeForDimension(new long[] { -1, 1, 2, 5 }, createStarNode, createNullNode, List.of(fixedLengthStarTreeNode -> {
                    try {
                        boolean result = true;
                        FixedLengthStarTreeNode lastMatchedNode;
                        lastMatchedNode = (FixedLengthStarTreeNode) fixedLengthStarTreeNode.getChildForDimensionValue(-1L);
                        result &= -1 == lastMatchedNode.getDimensionValue();
                        result &= null == lastMatchedNode.getChildForDimensionValue(5L);
                        result &= null == lastMatchedNode.getChildForDimensionValue(5L, lastMatchedNode);
                        lastMatchedNode = (FixedLengthStarTreeNode) fixedLengthStarTreeNode.getChildForDimensionValue(1L, lastMatchedNode);
                        result &= 1 == lastMatchedNode.getDimensionValue();
                        lastMatchedNode = (FixedLengthStarTreeNode) fixedLengthStarTreeNode.getChildForDimensionValue(5L, lastMatchedNode);
                        result &= 5 == lastMatchedNode.getDimensionValue();
                        lastMatchedNode = (FixedLengthStarTreeNode) fixedLengthStarTreeNode.getChildForDimensionValue(2L, lastMatchedNode);
                        result &= null == lastMatchedNode;
                        result &= null == fixedLengthStarTreeNode.getChildForDimensionValue(null);
                        result &= null == fixedLengthStarTreeNode.getChildForDimensionValue(null, null);
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
                int nullNodeLengthContribution = 0;

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
                    rootNode.addChildNode(nullNode, (long) ALL);
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

}

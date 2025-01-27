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
        createStarTreeForDimension(new long[] { 1, 2 }, true, true, List.of(fixedLengthStarTreeNode -> {
            try {
                boolean result = true;
                result &= 1 == fixedLengthStarTreeNode.getChildForDimensionValue(1L).getDimensionValue();
                result &= 2 == fixedLengthStarTreeNode.getChildForDimensionValue(2L).getDimensionValue();
                return result;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));

    }

    private void createStarTreeForDimension(
        long[] dimensionValues,
        boolean createStarNode,
        boolean createNullNode,
        List<Predicate<FixedLengthStarTreeNode>> predicates
    ) {

        try (Directory directory = newFSDirectory(createTempDir())) {

            long starTreeDataLength = 0;

            try (IndexOutput dataOut = directory.createOutput("star-tree-data", IOContext.DEFAULT)) {
                StarTreeWriter starTreeWriter = new StarTreeWriter();

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

                starTreeDataLength = starTreeWriter.writeStarTree(dataOut, rootNode, 2 + rootNode.getChildren().size(), "star-tree");

                // asserting on the actual length of the star tree data file
                assertEquals(starTreeDataLength, 33L * rootNode.getChildren().size() + 2 * 33);
            }

            try (IndexInput dataIn = directory.openInput("star-tree-data", IOContext.READONCE)) {
                StarTreeMetadata starTreeMetadata = mock(StarTreeMetadata.class);
                when(starTreeMetadata.getDataLength()).thenReturn(starTreeDataLength);
                when(starTreeMetadata.getDataStartFilePointer()).thenReturn(0L);
                FixedLengthStarTreeNode effectiveRoot = (FixedLengthStarTreeNode) StarTreeFactory.createStarTree(dataIn, starTreeMetadata);
                for (Predicate<FixedLengthStarTreeNode> predicate : predicates) {
                    assertTrue(predicate.test(effectiveRoot));
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}

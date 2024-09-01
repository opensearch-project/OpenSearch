/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.fileformats.data;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.StarTreeWriter;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.StarTreeMetadata;
import org.opensearch.index.compositeindex.datacube.startree.node.InMemoryTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeFactory;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNodeType;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StarTreeFileFormatsTests extends OpenSearchTestCase {

    private IndexOutput dataOut;
    private IndexInput dataIn;
    private Directory directory;
    private Integer maxLevels;
    private static Integer dimensionValue;

    @Before
    public void setup() throws IOException {
        directory = newFSDirectory(createTempDir());
        maxLevels = randomIntBetween(2, 5);
        dimensionValue = 0;
    }

    public void test_StarTreeNode() throws IOException {

        dataOut = directory.createOutput("star-tree-data", IOContext.DEFAULT);
        Map<Long, InMemoryTreeNode> inMemoryTreeNodeMap = new LinkedHashMap<>();
        InMemoryTreeNode root = generateSampleTree(inMemoryTreeNodeMap);
        StarTreeWriter starTreeWriter = new StarTreeWriter();
        long starTreeDataLength = starTreeWriter.writeStarTree(dataOut, root, inMemoryTreeNodeMap.size(), "star-tree");

        // asserting on the actual length of the star tree data file
        assertEquals(starTreeDataLength, (inMemoryTreeNodeMap.size() * 33L));
        dataOut.close();

        dataIn = directory.openInput("star-tree-data", IOContext.READONCE);

        StarTreeMetadata starTreeMetadata = mock(StarTreeMetadata.class);
        when(starTreeMetadata.getDataLength()).thenReturn(starTreeDataLength);
        when(starTreeMetadata.getDataStartFilePointer()).thenReturn(0L);

        StarTreeNode starTreeNode = StarTreeFactory.createStarTree(dataIn, starTreeMetadata);
        Queue<StarTreeNode> queue = new ArrayDeque<>();
        queue.add(starTreeNode);

        while ((starTreeNode = queue.poll()) != null) {

            // verify the star node
            assertStarTreeNode(starTreeNode, inMemoryTreeNodeMap.get(starTreeNode.getDimensionValue()));

            Iterator<? extends StarTreeNode> childrenIterator = starTreeNode.getChildrenIterator();

            if (starTreeNode.getChildDimensionId() != -1) {
                while (childrenIterator.hasNext()) {
                    StarTreeNode child = childrenIterator.next();
                    if (child.getStarTreeNodeType() == StarTreeNodeType.DEFAULT.getValue()) {
                        assertStarTreeNode(
                            starTreeNode.getChildForDimensionValue(child.getDimensionValue()),
                            inMemoryTreeNodeMap.get(child.getDimensionValue())
                        );
                        assertNull(starTreeNode.getChildStarNode());
                    }

                    queue.add(child);
                }
            } else {
                assertTrue(starTreeNode.isLeaf());
            }
        }

        dataIn.close();

    }

    public void test_starTreeSearch() throws IOException {

        dataOut = directory.createOutput("star-tree-data", IOContext.DEFAULT);
        Map<Long, InMemoryTreeNode> inMemoryTreeNodeMap = new LinkedHashMap<>();
        InMemoryTreeNode root = generateSampleTree(inMemoryTreeNodeMap);
        StarTreeWriter starTreeWriter = new StarTreeWriter();
        long starTreeDataLength = starTreeWriter.writeStarTree(dataOut, root, inMemoryTreeNodeMap.size(), "star-tree");

        // asserting on the actual length of the star tree data file
        assertEquals(starTreeDataLength, (inMemoryTreeNodeMap.size() * 33L));
        dataOut.close();

        dataIn = directory.openInput("star-tree-data", IOContext.READONCE);

        StarTreeMetadata starTreeMetadata = mock(StarTreeMetadata.class);
        when(starTreeMetadata.getDataLength()).thenReturn(starTreeDataLength);
        when(starTreeMetadata.getDataStartFilePointer()).thenReturn(0L);

        StarTreeNode starTreeNode = StarTreeFactory.createStarTree(dataIn, starTreeMetadata);
        InMemoryTreeNode inMemoryTreeNode = inMemoryTreeNodeMap.get(starTreeNode.getDimensionValue());
        assertNotNull(inMemoryTreeNode);

        for (int i = 0; i < maxLevels - 1; i++) {
            InMemoryTreeNode randomChildNode = randomFrom(inMemoryTreeNode.children.values());
            StarTreeNode randomStarTreeChildNode = starTreeNode.getChildForDimensionValue(randomChildNode.getDimensionValue());

            assertNotNull(randomStarTreeChildNode);
            assertStarTreeNode(randomStarTreeChildNode, randomChildNode);

            starTreeNode = randomStarTreeChildNode;
            inMemoryTreeNode = randomChildNode;

        }
        dataIn.close();
    }

    private void assertStarTreeNode(StarTreeNode starTreeNode, InMemoryTreeNode treeNode) throws IOException {
        assertEquals(starTreeNode.getDimensionId(), treeNode.dimensionId);
        assertEquals(starTreeNode.getDimensionValue(), treeNode.dimensionValue);
        assertEquals(starTreeNode.getStartDocId(), treeNode.startDocId);
        assertEquals(starTreeNode.getEndDocId(), treeNode.endDocId);
        assertEquals(starTreeNode.getChildDimensionId(), treeNode.childDimensionId);
        assertEquals(starTreeNode.getAggregatedDocId(), treeNode.aggregatedDocId);
        assertEquals(starTreeNode.getStarTreeNodeType(), treeNode.nodeType);

        if (starTreeNode.getChildDimensionId() != -1) {
            assertFalse(starTreeNode.isLeaf());
            if (treeNode.children != null) {
                assertEquals(starTreeNode.getNumChildren(), treeNode.children.values().size());
            }
        } else {
            assertTrue(starTreeNode.isLeaf());
        }

    }

    public InMemoryTreeNode generateSampleTree(Map<Long, InMemoryTreeNode> inMemoryTreeNodeMap) {
        // Create the root node
        InMemoryTreeNode root = new InMemoryTreeNode();
        root.dimensionId = 0;
        root.startDocId = randomInt();
        root.endDocId = randomInt();
        root.childDimensionId = 1;
        root.aggregatedDocId = randomInt();
        root.nodeType = (byte) 0;
        root.children = new HashMap<>();

        inMemoryTreeNodeMap.put(root.dimensionValue, root);

        // Generate the tree recursively
        generateTreeRecursively(root, 1, inMemoryTreeNodeMap);

        return root;
    }

    private void generateTreeRecursively(InMemoryTreeNode parent, int currentLevel, Map<Long, InMemoryTreeNode> inMemoryTreeNodeMap) {
        if (currentLevel >= this.maxLevels) {
            return; // Maximum level reached, stop generating children
        }

        int numChildren = randomIntBetween(1, 10);

        for (int i = 0; i < numChildren; i++) {
            InMemoryTreeNode child = new InMemoryTreeNode();
            dimensionValue++;
            child.dimensionId = currentLevel;
            child.dimensionValue = dimensionValue; // Assign a unique dimension value for each child
            child.startDocId = randomInt();
            child.endDocId = randomInt();
            child.childDimensionId = (currentLevel == this.maxLevels - 1) ? -1 : (currentLevel + 1);
            child.aggregatedDocId = randomInt();
            child.nodeType = (byte) 0;
            child.children = new HashMap<>();

            parent.children.put(child.dimensionValue, child);
            inMemoryTreeNodeMap.put(child.dimensionValue, child);

            generateTreeRecursively(child, currentLevel + 1, inMemoryTreeNodeMap);
        }
    }

    public void tearDown() throws Exception {
        super.tearDown();
        dataIn.close();
        dataOut.close();
        directory.close();
    }

}

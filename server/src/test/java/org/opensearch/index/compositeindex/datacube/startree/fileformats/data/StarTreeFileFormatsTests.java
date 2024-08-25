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
import org.opensearch.index.compositeindex.datacube.startree.node.StarTree;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
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

    @Before
    public void setup() throws IOException {
        directory = newFSDirectory(createTempDir());
    }

    public void test_StarTreeNode() throws IOException {

        dataOut = directory.createOutput("star-tree-data", IOContext.DEFAULT);
        Map<Long, InMemoryTreeNode> levelOrderStarTreeNodeMap = new LinkedHashMap<>();
        InMemoryTreeNode root = generateSampleTree(levelOrderStarTreeNodeMap);
        StarTreeWriter starTreeWriter = new StarTreeWriter();
        long starTreeDataLength = starTreeWriter.writeStarTree(dataOut, root, 7, "star-tree");

        // asserting on the actual length of the star tree data file
        assertEquals(starTreeDataLength, 247);
        dataOut.close();

        dataIn = directory.openInput("star-tree-data", IOContext.READONCE);

        StarTreeMetadata starTreeMetadata = mock(StarTreeMetadata.class);
        when(starTreeMetadata.getDataLength()).thenReturn(starTreeDataLength);
        when(starTreeMetadata.getDataStartFilePointer()).thenReturn(0L);
        StarTree starTree = new StarTree(dataIn, starTreeMetadata);

        StarTreeNode starTreeNode = starTree.getRoot();
        Queue<StarTreeNode> queue = new ArrayDeque<>();
        queue.add(starTreeNode);

        while ((starTreeNode = queue.poll()) != null) {

            // verify the star node
            assertStarTreeNode(starTreeNode, levelOrderStarTreeNodeMap.get(starTreeNode.getDimensionValue()));

            Iterator<? extends StarTreeNode> childrenIterator = starTreeNode.getChildrenIterator();

            if (starTreeNode.getChildDimensionId() != -1) {
                while (childrenIterator.hasNext()) {
                    StarTreeNode child = childrenIterator.next();
                    assertStarTreeNode(
                        starTreeNode.getChildForDimensionValue(child.getDimensionValue(), false),
                        levelOrderStarTreeNodeMap.get(child.getDimensionValue())
                    );
                    queue.add(child);
                }
            }
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

        if (starTreeNode.getChildDimensionId() != -1) {
            assertFalse(starTreeNode.isLeaf());
            if (treeNode.children != null) {
                assertEquals(starTreeNode.getNumChildren(), treeNode.children.values().size());
            }
        } else {
            assertTrue(starTreeNode.isLeaf());
        }

    }

    private InMemoryTreeNode generateSampleTree(Map<Long, InMemoryTreeNode> levelOrderStarTreeNode) {
        // Create the root node
        InMemoryTreeNode root = new InMemoryTreeNode();
        root.dimensionId = 0;
        root.startDocId = 0;
        root.endDocId = 100;
        root.childDimensionId = 1;
        root.aggregatedDocId = randomInt();
        root.nodeType = (byte) 0;
        root.children = new HashMap<>();

        levelOrderStarTreeNode.put(root.dimensionValue, root);

        // Create child nodes for dimension 1
        InMemoryTreeNode dim1Node1 = new InMemoryTreeNode();
        dim1Node1.dimensionId = 1;
        dim1Node1.dimensionValue = 1;
        dim1Node1.startDocId = 0;
        dim1Node1.endDocId = 50;
        dim1Node1.childDimensionId = 2;
        dim1Node1.aggregatedDocId = randomInt();
        root.nodeType = (byte) 0;
        dim1Node1.children = new HashMap<>();

        InMemoryTreeNode dim1Node2 = new InMemoryTreeNode();
        dim1Node2.dimensionId = 1;
        dim1Node2.dimensionValue = 2;
        dim1Node2.startDocId = 50;
        dim1Node2.endDocId = 100;
        dim1Node2.childDimensionId = 2;
        dim1Node2.aggregatedDocId = randomInt();
        root.nodeType = (byte) 0;
        dim1Node2.children = new HashMap<>();

        root.children.put(1L, dim1Node1);
        root.children.put(2L, dim1Node2);

        levelOrderStarTreeNode.put(dim1Node1.dimensionValue, dim1Node1);
        levelOrderStarTreeNode.put(dim1Node2.dimensionValue, dim1Node2);

        // Create child nodes for dimension 2
        InMemoryTreeNode dim2Node1 = new InMemoryTreeNode();
        dim2Node1.dimensionId = 2;
        dim2Node1.dimensionValue = 3;
        dim2Node1.startDocId = 0;
        dim2Node1.endDocId = 25;
        dim2Node1.childDimensionId = -1;
        dim2Node1.aggregatedDocId = randomInt();
        root.nodeType = (byte) 0;
        dim2Node1.children = null;

        InMemoryTreeNode dim2Node2 = new InMemoryTreeNode();
        dim2Node2.dimensionId = 2;
        dim2Node2.dimensionValue = 4;
        dim2Node2.startDocId = 25;
        dim2Node2.endDocId = 50;
        dim2Node2.childDimensionId = -1;
        dim2Node2.aggregatedDocId = randomInt();
        root.nodeType = (byte) 0;
        dim2Node2.children = null;

        InMemoryTreeNode dim2Node3 = new InMemoryTreeNode();
        dim2Node3.dimensionId = 2;
        dim2Node3.dimensionValue = 5;
        dim2Node3.startDocId = 50;
        dim2Node3.endDocId = 75;
        dim2Node3.childDimensionId = -1;
        dim2Node3.aggregatedDocId = randomInt();
        root.nodeType = (byte) 0;
        dim2Node3.children = null;

        InMemoryTreeNode dim2Node4 = new InMemoryTreeNode();
        dim2Node4.dimensionId = 2;
        dim2Node4.dimensionValue = 6;
        dim2Node4.startDocId = 75;
        dim2Node4.endDocId = 100;
        dim2Node4.childDimensionId = -1;
        dim2Node4.aggregatedDocId = randomInt();
        root.nodeType = (byte) 0;
        dim2Node4.children = null;

        dim1Node1.children.put(3L, dim2Node1);
        dim1Node1.children.put(4L, dim2Node2);
        dim1Node2.children.put(5L, dim2Node3);
        dim1Node2.children.put(6L, dim2Node4);

        levelOrderStarTreeNode.put(dim2Node1.dimensionValue, dim2Node1);
        levelOrderStarTreeNode.put(dim2Node2.dimensionValue, dim2Node2);
        levelOrderStarTreeNode.put(dim2Node3.dimensionValue, dim2Node3);
        levelOrderStarTreeNode.put(dim2Node4.dimensionValue, dim2Node4);

        return root;
    }

    public void tearDown() throws Exception {
        super.tearDown();
        dataIn.close();
        dataOut.close();
        directory.close();
    }

}

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
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FixedLengthStarTreeNodeTests extends OpenSearchTestCase {

    private IndexOutput dataOut;
    private IndexInput dataIn;
    private Directory directory;
    InMemoryTreeNode node;
    InMemoryTreeNode starChild;
    InMemoryTreeNode nullChild;
    FixedLengthStarTreeNode starTreeNode;

    @Before
    public void setup() throws IOException {
        directory = newFSDirectory(createTempDir());

        dataOut = directory.createOutput("star-tree-data", IOContext.DEFAULT);
        StarTreeWriter starTreeWriter = new StarTreeWriter();

        node = new InMemoryTreeNode();
        node.dimensionId = 0;
        node.startDocId = randomInt();
        node.endDocId = randomInt();
        node.childDimensionId = 1;
        node.aggregatedDocId = randomInt();
        node.nodeType = randomFrom((byte) 0, (byte) -1, (byte) 2);
        node.children = new HashMap<>();

        starChild = new InMemoryTreeNode();
        starChild.dimensionId = node.dimensionId + 1;
        starChild.dimensionValue = -1;
        starChild.startDocId = randomInt();
        starChild.endDocId = randomInt();
        starChild.childDimensionId = -1;
        starChild.aggregatedDocId = randomInt();
        starChild.nodeType = (byte) -2;
        starChild.children = new HashMap<>();
        node.children.put(-1L, starChild);

        nullChild = new InMemoryTreeNode();
        nullChild.dimensionId = node.dimensionId + 1;
        nullChild.dimensionValue = -1;
        nullChild.startDocId = randomInt();
        nullChild.endDocId = randomInt();
        nullChild.childDimensionId = -1;
        nullChild.aggregatedDocId = randomInt();
        nullChild.nodeType = (byte) -1;
        nullChild.children = new HashMap<>();
        node.children.put(null, nullChild);

        for (int i = 1; i < randomIntBetween(2, 5); i++) {
            InMemoryTreeNode child = new InMemoryTreeNode();
            child.dimensionId = node.dimensionId + 1;
            child.dimensionValue = node.dimensionValue + i; // Assign a unique dimension value for each child
            child.startDocId = randomInt();
            child.endDocId = randomInt();
            child.childDimensionId = -1;
            child.aggregatedDocId = randomInt();
            child.nodeType = (byte) 0;
            child.children = new HashMap<>();
            node.children.put(child.dimensionValue, child);
        }

        long starTreeDataLength = starTreeWriter.writeStarTree(dataOut, node, 1 + node.children.size(), "star-tree");

        // asserting on the actual length of the star tree data file
        assertEquals(starTreeDataLength, 33L * node.children.size() + 33);
        dataOut.close();

        dataIn = directory.openInput("star-tree-data", IOContext.READONCE);
        StarTreeMetadata starTreeMetadata = mock(StarTreeMetadata.class);
        when(starTreeMetadata.getDataLength()).thenReturn(starTreeDataLength);
        when(starTreeMetadata.getDataStartFilePointer()).thenReturn(0L);

        starTreeNode = (FixedLengthStarTreeNode) StarTreeFactory.createStarTree(dataIn, starTreeMetadata);

    }

    public void testOffsets() {
        assertEquals(0, FixedLengthStarTreeNode.DIMENSION_ID_OFFSET);
        assertEquals(4, FixedLengthStarTreeNode.DIMENSION_VALUE_OFFSET);
        assertEquals(12, FixedLengthStarTreeNode.START_DOC_ID_OFFSET);
        assertEquals(16, FixedLengthStarTreeNode.END_DOC_ID_OFFSET);
        assertEquals(20, FixedLengthStarTreeNode.AGGREGATE_DOC_ID_OFFSET);
        assertEquals(24, FixedLengthStarTreeNode.STAR_NODE_TYPE_OFFSET);
        assertEquals(25, FixedLengthStarTreeNode.FIRST_CHILD_ID_OFFSET);
        assertEquals(29, FixedLengthStarTreeNode.LAST_CHILD_ID_OFFSET);
    }

    public void testSerializableDataSize() {
        assertEquals(33, FixedLengthStarTreeNode.SERIALIZABLE_DATA_SIZE_IN_BYTES);
    }

    public void testGetDimensionId() throws IOException {
        assertEquals(node.dimensionId, starTreeNode.getDimensionId());
    }

    public void testGetDimensionValue() throws IOException {
        assertEquals(node.dimensionValue, starTreeNode.getDimensionValue());
    }

    public void testGetStartDocId() throws IOException {
        assertEquals(node.startDocId, starTreeNode.getStartDocId());
    }

    public void testGetEndDocId() throws IOException {
        assertEquals(node.endDocId, starTreeNode.getEndDocId());
    }

    public void testGetAggregatedDocId() throws IOException {
        assertEquals(node.aggregatedDocId, starTreeNode.getAggregatedDocId());
    }

    public void testGetNumChildren() throws IOException {
        assertEquals(node.children.size(), starTreeNode.getNumChildren());
    }

    public void testIsLeaf() {
        assertFalse(starTreeNode.isLeaf());
    }

    public void testGetStarTreeNodeType() throws IOException {
        assertEquals(node.getNodeType(), starTreeNode.getStarTreeNodeType());
    }

    public void testGetChildForDimensionValue() throws IOException {
        // TODO: Add a test to verify children with star node, null node and default node with default dimension value -1
        long dimensionValue = randomIntBetween(0, node.children.size() - 3);
        FixedLengthStarTreeNode childNode = (FixedLengthStarTreeNode) starTreeNode.getChildForDimensionValue(dimensionValue);
        assertNotNull(childNode);
        assertEquals(dimensionValue, childNode.getDimensionValue());
    }

    public void testGetChildrenIterator() throws IOException {
        Iterator<FixedLengthStarTreeNode> iterator = starTreeNode.getChildrenIterator();
        int count = 0;
        while (iterator.hasNext()) {
            FixedLengthStarTreeNode child = iterator.next();
            assertNotNull(child);
            count++;
        }
        assertEquals(starTreeNode.getNumChildren(), count);
    }

    public void testGetChildForStarNode() throws IOException {
        // Assuming the first child is a star node in our test data
        FixedLengthStarTreeNode starNode = (FixedLengthStarTreeNode) starTreeNode.getChildStarNode();
        assertNotNull(starNode);
        assertEquals(StarTreeUtils.ALL, starNode.getDimensionValue());
    }

    public void testGetChildForNullNode() throws IOException {
        FixedLengthStarTreeNode nullNode = (FixedLengthStarTreeNode) starTreeNode.getChildForDimensionValue(null);
        assertNull(nullNode);
    }

    public void testGetChildForInvalidDimensionValue() throws IOException {
        long invalidDimensionValue = Long.MAX_VALUE;
        assertThrows(AssertionError.class, () -> starTreeNode.getChildForDimensionValue(invalidDimensionValue));
    }

    public void testOnlyRootNodePresent() throws IOException {

        Directory directory = newFSDirectory(createTempDir());

        IndexOutput dataOut = directory.createOutput("star-tree-data-1", IOContext.DEFAULT);
        StarTreeWriter starTreeWriter = new StarTreeWriter();

        InMemoryTreeNode node = new InMemoryTreeNode();
        node.dimensionId = 0;
        node.startDocId = randomInt();
        node.endDocId = randomInt();
        node.childDimensionId = 1;
        node.aggregatedDocId = randomInt();
        node.nodeType = randomFrom((byte) 0, (byte) -1, (byte) 2);
        node.children = new HashMap<>();

        long starTreeDataLength = starTreeWriter.writeStarTree(dataOut, node, 1, "star-tree");

        // asserting on the actual length of the star tree data file
        assertEquals(starTreeDataLength, 33);
        dataOut.close();

        IndexInput dataIn = directory.openInput("star-tree-data-1", IOContext.READONCE);
        StarTreeMetadata starTreeMetadata = mock(StarTreeMetadata.class);
        when(starTreeMetadata.getDataLength()).thenReturn(starTreeDataLength);
        when(starTreeMetadata.getDataStartFilePointer()).thenReturn(0L);

        FixedLengthStarTreeNode starTreeNode = (FixedLengthStarTreeNode) StarTreeFactory.createStarTree(dataIn, starTreeMetadata);

        assertEquals(starTreeNode.getNumChildren(), 0);
        assertNull(starTreeNode.getChildForDimensionValue(randomLong()));
        assertThrows(IllegalArgumentException.class, () -> starTreeNode.getChildrenIterator().next());
        assertThrows(UnsupportedOperationException.class, () -> starTreeNode.getChildrenIterator().remove());

        dataIn.close();
        directory.close();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        dataIn.close();
        dataOut.close();
        directory.close();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.fileformats.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNodeType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Fixed Length implementation of {@link StarTreeNode}.
 * <p>
 * This class represents a node in a star tree with a fixed-length serialization format.
 * It provides efficient storage and retrieval of node information using a RandomAccessInput.
 * The node structure includes the methods to access all the constructs of InMemoryTreeNode.
 *
 * <p>
 * Key features:
 * - Fixed-size serialization for each node, allowing for efficient random access
 * - Binary search capability for finding child nodes
 * - Support for star nodes, null nodes and other default nodes
 * - Iteration over child nodes
 * <p>
 * The class uses specific byte offsets for each field in the serialized format,
 * enabling direct access to node properties without parsing the entire node structure.
 *
 * @opensearch.experimental
 */
public class FixedLengthStarTreeNode implements StarTreeNode {

    private static final Logger logger = LogManager.getLogger(FixedLengthStarTreeNode.class);

    /**
     * Number of integer fields in the serializable data
     */
    public static final int NUM_INT_SERIALIZABLE_FIELDS = 6;

    /**
     * Number of long fields in the serializable data
     */
    public static final int NUM_LONG_SERIALIZABLE_FIELDS = 1;

    /**
     * Number of byte fields in the serializable data
     */
    public static final int NUM_BYTE_SERIALIZABLE_FIELDS = 1;

    /**
     * Total size in bytes of the serializable data for each node
     */
    public static final long SERIALIZABLE_DATA_SIZE_IN_BYTES = (Integer.BYTES * NUM_INT_SERIALIZABLE_FIELDS) + (Long.BYTES
        * NUM_LONG_SERIALIZABLE_FIELDS) + (NUM_BYTE_SERIALIZABLE_FIELDS * Byte.BYTES);

    // Byte offsets for each field in the serialized data
    static final int DIMENSION_ID_OFFSET = 0;
    static final int DIMENSION_VALUE_OFFSET = DIMENSION_ID_OFFSET + Integer.BYTES;
    static final int START_DOC_ID_OFFSET = DIMENSION_VALUE_OFFSET + Long.BYTES;
    static final int END_DOC_ID_OFFSET = START_DOC_ID_OFFSET + Integer.BYTES;
    static final int AGGREGATE_DOC_ID_OFFSET = END_DOC_ID_OFFSET + Integer.BYTES;
    static final int STAR_NODE_TYPE_OFFSET = AGGREGATE_DOC_ID_OFFSET + Integer.BYTES;
    static final int FIRST_CHILD_ID_OFFSET = STAR_NODE_TYPE_OFFSET + Byte.BYTES;
    static final int LAST_CHILD_ID_OFFSET = FIRST_CHILD_ID_OFFSET + Integer.BYTES;

    /**
     * Constant representing an invalid node ID
     */
    public static final int INVALID_ID = -1;

    /**
     * The ID of this node
     */
    private final int nodeId;

    /**
     * The ID of the first child of this node
     */
    private final int firstChildId;

    /**
     * The input source for reading node data
     */
    RandomAccessInput in;

    /**
     * Constructs a FixedLengthStarTreeNode.
     *
     * @param in     The RandomAccessInput to read node data from
     * @param nodeId The ID of this node
     * @throws IOException If there's an error reading from the input
     */
    public FixedLengthStarTreeNode(RandomAccessInput in, int nodeId) throws IOException {
        this.in = in;
        this.nodeId = nodeId;
        firstChildId = getInt(FIRST_CHILD_ID_OFFSET);
    }

    /**
     * Reads an integer value from the specified offset in the node's data.
     *
     * @param fieldOffset The offset of the field to read
     * @return The integer value at the specified offset
     * @throws IOException If there's an error reading from the input
     */
    private int getInt(int fieldOffset) throws IOException {
        return in.readInt(nodeId * SERIALIZABLE_DATA_SIZE_IN_BYTES + fieldOffset);
    }

    /**
     * Reads a long value from the specified offset in the node's data.
     *
     * @param fieldOffset The offset of the field to read
     * @return The long value at the specified offset
     * @throws IOException If there's an error reading from the input
     */
    private long getLong(int fieldOffset) throws IOException {
        return in.readLong(nodeId * SERIALIZABLE_DATA_SIZE_IN_BYTES + fieldOffset);
    }

    /**
     * Reads a byte value from the specified offset in the node's data.
     *
     * @param fieldOffset The offset of the field to read
     * @return The byte value at the specified offset
     * @throws IOException If there's an error reading from the input
     */
    private byte getByte(int fieldOffset) throws IOException {
        return in.readByte(nodeId * SERIALIZABLE_DATA_SIZE_IN_BYTES + fieldOffset);
    }

    @Override
    public int getDimensionId() throws IOException {
        return getInt(DIMENSION_ID_OFFSET);
    }

    @Override
    public long getDimensionValue() throws IOException {
        return getLong(DIMENSION_VALUE_OFFSET);
    }

    @Override
    public int getChildDimensionId() throws IOException {
        if (firstChildId == INVALID_ID) {
            return INVALID_ID;
        } else {
            return in.readInt(firstChildId * SERIALIZABLE_DATA_SIZE_IN_BYTES);
        }
    }

    @Override
    public int getStartDocId() throws IOException {
        return getInt(START_DOC_ID_OFFSET);
    }

    @Override
    public int getEndDocId() throws IOException {
        return getInt(END_DOC_ID_OFFSET);
    }

    @Override
    public int getAggregatedDocId() throws IOException {
        return getInt(AGGREGATE_DOC_ID_OFFSET);
    }

    @Override
    public int getNumChildren() throws IOException {
        if (firstChildId == INVALID_ID) {
            return 0;
        } else {
            return getInt(LAST_CHILD_ID_OFFSET) - firstChildId + 1;
        }
    }

    @Override
    public boolean isLeaf() {
        return firstChildId == INVALID_ID;
    }

    @Override
    public byte getStarTreeNodeType() throws IOException {
        return getByte(STAR_NODE_TYPE_OFFSET);
    }

    @Override
    public StarTreeNode getChildStarNode() throws IOException {
        return handleStarNode();
    }

    @Override
    public StarTreeNode getChildForDimensionValue(Long dimensionValue) throws IOException {
        // there will be no children for leaf nodes
        if (isLeaf()) {
            return null;
        }

        StarTreeNode resultStarTreeNode = null;
        if (null != dimensionValue) {
            int resultStarTreeNodeId = binarySearchForDimension(dimensionValue, false);
            if (resultStarTreeNodeId != INVALID_ID) {
                resultStarTreeNode = new FixedLengthStarTreeNode(in, resultStarTreeNodeId);
            }
        }
        return resultStarTreeNode;
    }

    /**
     * Handles the special case of a star node.
     *
     * @return The star node if found, null otherwise
     * @throws IOException If there's an error reading from the input
     */
    private FixedLengthStarTreeNode handleStarNode() throws IOException {
        FixedLengthStarTreeNode firstNode = new FixedLengthStarTreeNode(in, firstChildId);
        return matchStarTreeNodeTypeOrNull(firstNode, StarTreeNodeType.STAR);
    }

    /**
     * Checks if the given node matches the specified StarTreeNodeType.
     *
     * @param firstNode        The FixedLengthStarTreeNode to check.
     * @param starTreeNodeType The StarTreeNodeType to match against.
     * @return The firstNode if its type matches the targetType, null otherwise.
     * @throws IOException If an I/O error occurs during the operation.
     */
    private static FixedLengthStarTreeNode matchStarTreeNodeTypeOrNull(FixedLengthStarTreeNode firstNode, StarTreeNodeType starTreeNodeType)
        throws IOException {
        if (firstNode.getStarTreeNodeType() == starTreeNodeType.getValue()) {
            return firstNode;
        } else {
            return null;
        }
    }

    @Override
    public Iterator<FixedLengthStarTreeNode> getChildrenIterator() throws IOException {
        return new Iterator<>() {
            private int currentChildId = firstChildId;
            private final int lastChildId = getInt(LAST_CHILD_ID_OFFSET);

            @Override
            public boolean hasNext() {
                return currentChildId <= lastChildId;
            }

            @Override
            public FixedLengthStarTreeNode next() {
                try {
                    return new FixedLengthStarTreeNode(in, currentChildId++);
                } catch (IOException | RuntimeException e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Finds and returns all children with dimension values between the given start and end values.
     *
     * @param startDimensionValue The start of the range (inclusive)
     * @param endDimensionValue   The end of the range (inclusive)
     * @return A list of child nodes whose dimension values lie between the specified range
     * @throws IOException If there's an error reading from the input
     */
    public Iterator<FixedLengthStarTreeNode> range(long startDimensionValue, long endDimensionValue) throws IOException {
        return new Iterator<>() {
            int currentChildId = binarySearchForDimension(startDimensionValue, true);
            final int lastChildId = getInt(LAST_CHILD_ID_OFFSET);
            FixedLengthStarTreeNode nextNode = null;

            @Override
            public boolean hasNext() {
                try {
                    // Continue iterating while we have valid children
                    while (currentChildId != INVALID_ID && currentChildId <= lastChildId) {
                        FixedLengthStarTreeNode currentNode = new FixedLengthStarTreeNode(in, currentChildId);
                        long currentDimensionValue = currentNode.getDimensionValue();

                        // If node exceeds endDimensionValue, we are out of the range
                        if (currentDimensionValue > endDimensionValue) {
                            return false;
                        }

                        // If node is within the range, we prepare it as the next node
                        if (currentDimensionValue >= startDimensionValue) {
                            nextNode = currentNode;
                            currentChildId++; // Move to the next child for future iterations
                            return true;
                        }

                        // Otherwise, move to the next child
                        currentChildId++;
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return false; // No more children in range
            }

            @Override
            public FixedLengthStarTreeNode next() {
                if (nextNode == null && !hasNext()) {
                    throw new NoSuchElementException();
                }

                FixedLengthStarTreeNode returnNode = nextNode;
                nextNode = null; // Reset nextNode for the next call
                return returnNode;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Performs a binary search to find a node with a dimension value.
     *
     * @param dimensionValue   The dimension value to search for
     * @param findFirstInRange If true, find the first node >= dimensionValue; otherwise, find exact matches
     * @return The node ID if found, INVALID_ID otherwise
     * @throws IOException If there's an error reading from the input
     */
    private int binarySearchForDimension(long dimensionValue, boolean findFirstInRange) throws IOException {

        int low = firstChildId;
        // if the current node is star node, increment the low to reduce the search space
        if (isStarNode(low)) {
            low++;
        }

        int high = getInt(LAST_CHILD_ID_OFFSET);
        // if the current node is null node, decrement the high to reduce the search space
        if (isNullNode(high)) {
            high--;
        }

        int resultId = INVALID_ID;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            FixedLengthStarTreeNode midNode = new FixedLengthStarTreeNode(in, mid);
            long midDimensionValue = midNode.getDimensionValue();

            if (midDimensionValue == dimensionValue) {
                return mid; // Exact match found
            } else if (midDimensionValue < dimensionValue) {
                low = mid + 1;
            } else {
                resultId = mid; // Possible candidate for start of range
                high = mid - 1;
            }
        }

        return findFirstInRange ? resultId : INVALID_ID;
    }

    /**
     * Checks if the node at the given ID is a star node.
     *
     * @param nodeId The ID of the node to check
     * @return true if it's a star node, false otherwise
     * @throws IOException If there's an error reading from the input
     */
    private boolean isStarNode(int nodeId) throws IOException {
        return matchStarTreeNodeTypeOrNull(new FixedLengthStarTreeNode(in, nodeId), StarTreeNodeType.STAR) != null;
    }

    /**
     * Checks if the node at the given ID is a null node.
     *
     * @param nodeId The ID of the node to check
     * @return true if it's a null node, false otherwise
     * @throws IOException If there's an error reading from the input
     */
    private boolean isNullNode(int nodeId) throws IOException {
        return matchStarTreeNodeTypeOrNull(new FixedLengthStarTreeNode(in, nodeId), StarTreeNodeType.NULL) != null;
    }

}

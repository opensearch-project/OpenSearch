/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.node;

import org.apache.lucene.store.RandomAccessInput;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;

import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.ALL;

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
 *
 * The class uses specific byte offsets for each field in the serialized format,
 * enabling direct access to node properties without parsing the entire node structure.
 *
 * @opensearch.experimental
 */
public class FixedLengthStarTreeNode implements StarTreeNode {

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
    private static final int DIMENSION_ID_OFFSET = 0;
    private static final int DIMENSION_VALUE_OFFSET = DIMENSION_ID_OFFSET + Integer.BYTES;
    private static final int START_DOC_ID_OFFSET = DIMENSION_VALUE_OFFSET + Long.BYTES;
    private static final int END_DOC_ID_OFFSET = START_DOC_ID_OFFSET + Integer.BYTES;
    private static final int AGGREGATE_DOC_ID_OFFSET = END_DOC_ID_OFFSET + Integer.BYTES;
    private static final int STAR_NODE_TYPE_OFFSET = AGGREGATE_DOC_ID_OFFSET + Integer.BYTES;
    private static final int FIRST_CHILD_ID_OFFSET = STAR_NODE_TYPE_OFFSET + Byte.BYTES;
    private static final int LAST_CHILD_ID_OFFSET = FIRST_CHILD_ID_OFFSET + Integer.BYTES;

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
    public StarTreeNode getChildForDimensionValue(long dimensionValue, boolean isStar) throws IOException {
        // there will be no children for leaf nodes
        if (isLeaf()) {
            return null;
        }

        // Specialize star node for performance
        if (isStar) {
            return handleStarNode();
        }

        return binarySearchChild(dimensionValue);
    }

    /**
     * Handles the special case of a star node.
     *
     * @return The star node if found, null otherwise
     * @throws IOException If there's an error reading from the input
     */
    private FixedLengthStarTreeNode handleStarNode() throws IOException {
        FixedLengthStarTreeNode firstNode = new FixedLengthStarTreeNode(in, firstChildId);
        if (firstNode.getDimensionValue() == ALL) {
            return firstNode;
        } else {
            return null;
        }
    }

    /**
     * Performs a binary search to find a child node with the given dimension value.
     *
     * @param dimensionValue The dimension value to search for
     * @return The child node if found, null otherwise
     * @throws IOException If there's an error reading from the input
     */
    private FixedLengthStarTreeNode binarySearchChild(long dimensionValue) throws IOException {
        int low = firstChildId;
        int high = getInt(LAST_CHILD_ID_OFFSET);

        while (low <= high) {
            int mid = low + (high - low) / 2;
            FixedLengthStarTreeNode midNode = new FixedLengthStarTreeNode(in, mid);
            long midNodeDimensionValue = midNode.getDimensionValue();

            if (midNodeDimensionValue == dimensionValue) {
                return midNode;
            } else if (midNodeDimensionValue < dimensionValue) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return null;
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
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}

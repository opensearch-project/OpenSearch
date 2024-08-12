/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.fileformats.data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.compositeindex.datacube.startree.node.InMemoryTreeNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.opensearch.index.compositeindex.CompositeIndexConstants.COMPOSITE_FIELD_MARKER;
import static org.opensearch.index.compositeindex.datacube.startree.fileformats.StarTreeWriter.VERSION_CURRENT;
import static org.opensearch.index.compositeindex.datacube.startree.node.FixedLengthStarTreeNode.SERIALIZABLE_DATA_SIZE_IN_BYTES;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.ALL;

/**
 * Utility class for serializing a star-tree data structure.
 *
 * @opensearch.experimental
 */
public class StarTreeDataWriter {

    private static final Logger logger = LogManager.getLogger(StarTreeDataWriter.class);

    /**
     * Writes the star-tree data structure.
     *
     * @param indexOutput the IndexOutput to write the star-tree data
     * @param rootNode    the root node of the star-tree
     * @param numNodes    the total number of nodes in the star-tree
     * @param name        the name of the star-tree field
     * @return the total size in bytes of the serialized star-tree data
     * @throws IOException if an I/O error occurs while writing the star-tree data
     */
    public static long writeStarTree(IndexOutput indexOutput, InMemoryTreeNode rootNode, int numNodes, String name) throws IOException {
        long totalSizeInBytes = 0L;
        totalSizeInBytes += computeStarTreeDataHeaderByteSize();
        totalSizeInBytes += (long) numNodes * SERIALIZABLE_DATA_SIZE_IN_BYTES;

        if (logger.isDebugEnabled()) {
            logger.debug("Star tree data size in bytes : {} for star-tree field {}", totalSizeInBytes, name);
        }

        writeStarTreeHeader(indexOutput, numNodes);
        writeStarTreeNodes(indexOutput, rootNode);
        return totalSizeInBytes;
    }

    /**
     * Computes the byte size of the star-tree data header.
     *
     * @return the byte size of the star-tree data header
     */
    public static int computeStarTreeDataHeaderByteSize() {
        // Magic marker (8), version (4)
        int headerSizeInBytes = 12;

        // For number of nodes.
        headerSizeInBytes += Integer.BYTES;
        return headerSizeInBytes;
    }

    /**
     * Writes the star-tree data header.
     *
     * @param output   the IndexOutput to write the header
     * @param numNodes the total number of nodes in the star-tree
     * @throws IOException if an I/O error occurs while writing the header
     */
    private static void writeStarTreeHeader(IndexOutput output, int numNodes) throws IOException {
        output.writeLong(COMPOSITE_FIELD_MARKER);
        output.writeInt(VERSION_CURRENT);
        output.writeInt(numNodes);
    }

    /**
     * Writes the star-tree nodes in a breadth-first order.
     *
     * @param output   the IndexOutput to write the nodes
     * @param rootNode the root node of the star-tree
     * @throws IOException if an I/O error occurs while writing the nodes
     */
    private static void writeStarTreeNodes(IndexOutput output, InMemoryTreeNode rootNode) throws IOException {
        Queue<InMemoryTreeNode> queue = new LinkedList<>();
        queue.add(rootNode);

        int currentNodeId = 0;
        while (!queue.isEmpty()) {
            InMemoryTreeNode node = queue.remove();

            if (node.children == null || node.children.isEmpty()) {
                writeStarTreeNode(output, node, ALL, ALL);
            } else {

                // Sort all children nodes based on dimension value
                List<InMemoryTreeNode> sortedChildren = new ArrayList<>(node.children.values());
                sortedChildren.sort(
                    Comparator.comparingInt(InMemoryTreeNode::getNodeType).thenComparingLong(InMemoryTreeNode::getDimensionValue)
                );

                int firstChildId = currentNodeId + queue.size() + 1;
                int lastChildId = firstChildId + sortedChildren.size() - 1;
                writeStarTreeNode(output, node, firstChildId, lastChildId);

                queue.addAll(sortedChildren);
            }

            currentNodeId++;
        }
    }

    /**
     * Writes a single star-tree node
     *
     * @param output       the IndexOutput to write the node
     * @param node         the star tree node to write
     * @param firstChildId the ID of the first child node
     * @param lastChildId  the ID of the last child node
     * @throws IOException if an I/O error occurs while writing the node
     */
    private static void writeStarTreeNode(IndexOutput output, InMemoryTreeNode node, int firstChildId, int lastChildId) throws IOException {
        output.writeInt(node.dimensionId);
        output.writeLong(node.dimensionValue);
        output.writeInt(node.startDocId);
        output.writeInt(node.endDocId);
        output.writeInt(node.aggregatedDocId);
        output.writeByte(node.nodeType);
        output.writeInt(firstChildId);
        output.writeInt(lastChildId);
    }

}

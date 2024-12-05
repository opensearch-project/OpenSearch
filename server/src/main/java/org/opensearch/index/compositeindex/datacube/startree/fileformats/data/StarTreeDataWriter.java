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
import java.util.LinkedList;
import java.util.Queue;

import static org.opensearch.index.compositeindex.datacube.startree.fileformats.node.FixedLengthStarTreeNode.SERIALIZABLE_DATA_SIZE_IN_BYTES;
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
        long totalSizeInBytes = (long) numNodes * SERIALIZABLE_DATA_SIZE_IN_BYTES;

        logger.debug("Star tree data size in bytes : {} for star-tree field {}", totalSizeInBytes, name);

        writeStarTreeNodes(indexOutput, rootNode);
        return totalSizeInBytes;
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

            if (!node.hasChild()) {
                writeStarTreeNode(output, node, ALL, ALL);
            } else {

                int totalNumberOfChildren = 0;
                int firstChildId = currentNodeId + queue.size() + 1;

                if (node.getChildStarNode() != null) {
                    totalNumberOfChildren++;
                    queue.add(node.getChildStarNode());
                }

                if (node.getChildren() != null) {
                    totalNumberOfChildren = totalNumberOfChildren + node.getChildren().values().size();
                    queue.addAll(node.getChildren().values());
                }

                int lastChildId = firstChildId + totalNumberOfChildren - 1;
                writeStarTreeNode(output, node, firstChildId, lastChildId);

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
        output.writeInt(node.getDimensionId());
        output.writeLong(node.getDimensionValue());
        output.writeInt(node.getStartDocId());
        output.writeInt(node.getEndDocId());
        output.writeInt(node.getAggregatedDocId());
        output.writeByte(node.getNodeType());
        output.writeInt(firstChildId);
        output.writeInt(lastChildId);
    }

}

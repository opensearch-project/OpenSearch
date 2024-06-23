/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Util class for building star tree
 * @opensearch.experimental
 */
public class StarTreeBuilderUtils {

    private static final Logger logger = LogManager.getLogger(StarTreeBuilderUtils.class);

    // TODO: To be moved to off heap star tree implementation
    public static final int NUM_INT_SERIALIZABLE_FIELDS = 6;
    public static final int NUM_LONG_SERIALIZABLE_FIELDS = 1;
    public static final long SERIALIZABLE_SIZE_IN_BYTES = (Integer.BYTES * NUM_INT_SERIALIZABLE_FIELDS) + (Long.BYTES
        * NUM_LONG_SERIALIZABLE_FIELDS);

    private StarTreeBuilderUtils() {}

    public static final int ALL = -1;
    public static final long MAGIC_MARKER = 0xBADDA55B00DAD00DL;

    /** Tree node representation */
    public static class TreeNode {
        public int dimensionId = ALL;
        public long dimensionValue = ALL;
        public int startDocId = ALL;
        public int endDocId = ALL;
        public int aggregatedDocId = ALL;
        public int childDimensionId = ALL;
        public Map<Long, TreeNode> children;
    }

    /** Serializes the tree */
    public static void serializeTree(IndexOutput indexOutput, TreeNode rootNode, String[] dimensions, int numNodes) throws IOException {
        int headerSizeInBytes = computeHeaderByteSize(dimensions);
        long totalSizeInBytes = headerSizeInBytes + (long) numNodes * SERIALIZABLE_SIZE_IN_BYTES;

        logger.info("Star tree size in bytes : {}", totalSizeInBytes);

        writeHeader(indexOutput, headerSizeInBytes, dimensions, numNodes);
        writeNodes(indexOutput, rootNode);
    }

    /** Computes the size of the header for the tree */
    static int computeHeaderByteSize(String[] dimensions) {
        // Magic marker (8), version (4), size of header (4) and number of dimensions (4)
        int headerSizeInBytes = 20;

        for (String dimension : dimensions) {
            headerSizeInBytes += Integer.BYTES; // For dimension index
            headerSizeInBytes += Integer.BYTES; // For length of dimension name
            headerSizeInBytes += dimension.getBytes(UTF_8).length; // For dimension name
        }

        headerSizeInBytes += Integer.BYTES; // For number of nodes.
        return headerSizeInBytes;
    }

    /** Writes the header of the tree */
    static void writeHeader(IndexOutput output, int headerSizeInBytes, String[] dimensions, int numNodes) throws IOException {
        output.writeLong(MAGIC_MARKER);
        output.writeInt(1);
        output.writeInt(headerSizeInBytes);
        output.writeInt(dimensions.length);
        for (int i = 0; i < dimensions.length; i++) {
            output.writeInt(i);
            output.writeString(dimensions[i]);
        }
        output.writeInt(numNodes);
    }

    /** Writes the nodes of the tree */
    static void writeNodes(IndexOutput output, TreeNode rootNode) throws IOException {
        Queue<TreeNode> queue = new LinkedList<>();
        queue.add(rootNode);

        int currentNodeId = 0;
        while (!queue.isEmpty()) {
            TreeNode node = queue.remove();

            if (node.children == null) {
                writeNode(output, node, ALL, ALL);
            } else {
                // Sort all children nodes based on dimension value
                List<TreeNode> sortedChildren = new ArrayList<>(node.children.values());
                sortedChildren.sort(Comparator.comparingLong(o -> o.dimensionValue));

                int firstChildId = currentNodeId + queue.size() + 1;
                int lastChildId = firstChildId + sortedChildren.size() - 1;
                writeNode(output, node, firstChildId, lastChildId);

                queue.addAll(sortedChildren);
            }

            currentNodeId++;
        }
    }

    /** Writes a node of the tree */
    private static void writeNode(IndexOutput output, TreeNode node, int firstChildId, int lastChildId) throws IOException {
        output.writeInt(node.dimensionId);
        output.writeLong(node.dimensionValue);
        output.writeInt(node.startDocId);
        output.writeInt(node.endDocId);
        output.writeInt(node.aggregatedDocId);
        output.writeInt(firstChildId);
        output.writeInt(lastChildId);
    }

}

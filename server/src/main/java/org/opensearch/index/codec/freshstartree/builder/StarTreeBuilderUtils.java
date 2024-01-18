/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.index.codec.freshstartree.builder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IndexOutput;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.opensearch.index.codec.freshstartree.node.OffHeapStarTreeNode.SERIALIZABLE_SIZE_IN_BYTES;

// import static
// org.opensearch.index.codec.freshstartree.node.OffHeapStarTreeNode.SERIALIZABLE_SIZE_IN_BYTES;

/** Util class for building star tree */
public class StarTreeBuilderUtils {

    private static final Logger logger = LogManager.getLogger(StarTreeBuilderUtils.class);

    private StarTreeBuilderUtils() {
    }

    public static final int INVALID_ID = -1;
    public static final long MAGIC_MARKER = 0xBADDA55B00DAD00DL;

    /** Tree node representation */
    public static class TreeNode {
        public int _dimensionId = INVALID_ID;
        public long _dimensionValue = INVALID_ID;
        public int _startDocId = INVALID_ID;
        public int _endDocId = INVALID_ID;
        public int _aggregatedDocId = INVALID_ID;
        public int _childDimensionId = INVALID_ID;
        public Map<Long, TreeNode> _children;
    }

    public static void serializeTree(IndexOutput indexOutput, TreeNode rootNode, String[] dimensions, int numNodes)
        throws IOException {
        int headerSizeInBytes = computeHeaderByteSize(dimensions);
        long totalSizeInBytes = headerSizeInBytes + (long) numNodes * SERIALIZABLE_SIZE_IN_BYTES;

        logger.info("Star tree size in bytes : {}", totalSizeInBytes);

        writeHeader(indexOutput, headerSizeInBytes, dimensions, numNodes);
        writeNodes(indexOutput, rootNode);
    }

    private static int computeHeaderByteSize(String[] dimensions) {
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

    private static void writeHeader(IndexOutput output, int headerSizeInBytes, String[] dimensions, int numNodes)
        throws IOException {
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

    private static void writeNodes(IndexOutput output, TreeNode rootNode)
        throws IOException {
        Queue<TreeNode> queue = new LinkedList<>();
        queue.add(rootNode);

        int currentNodeId = 0;
        while (!queue.isEmpty()) {
            TreeNode node = queue.remove();

            if (node._children == null) {
                writeNode(output, node, INVALID_ID, INVALID_ID);
            } else {
                // Sort all children nodes based on dimension value
                List<TreeNode> sortedChildren = new ArrayList<>(node._children.values());
                sortedChildren.sort((o1, o2) -> Long.compare(o1._dimensionValue, o2._dimensionValue));

                int firstChildId = currentNodeId + queue.size() + 1;
                int lastChildId = firstChildId + sortedChildren.size() - 1;
                writeNode(output, node, firstChildId, lastChildId);

                queue.addAll(sortedChildren);
            }

            currentNodeId++;
        }
    }

    private static void writeNode(IndexOutput output, TreeNode node, int firstChildId, int lastChildId)
        throws IOException {
        output.writeInt(node._dimensionId);
        output.writeLong(node._dimensionValue);
        output.writeInt(node._startDocId);
        output.writeInt(node._endDocId);
        output.writeInt(node._aggregatedDocId);
        output.writeInt(firstChildId);
        output.writeInt(lastChildId);
    }
}

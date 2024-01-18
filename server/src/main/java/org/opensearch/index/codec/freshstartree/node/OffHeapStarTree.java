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
package org.opensearch.index.codec.freshstartree.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;


/** Off heap implementation of star tree. */
public class OffHeapStarTree implements StarTree {
    public static final long MAGIC_MARKER = 0xBADDA55B00DAD00DL;
    public static final int VERSION = 1;
    private final OffHeapStarTreeNode _root;
    private final List<String> _dimensionNames = new ArrayList<>();

    public OffHeapStarTree(IndexInput data)
        throws IOException {
        long magicmarker = data.readLong();
        if (MAGIC_MARKER != magicmarker) {
            throw new IOException("Invalid magic marker");
        }
        int ver = data.readInt();
        if (VERSION != ver) {
            throw new IOException("Invalid version");
        }
        data.readInt(); // header size

        int dimLength = data.readInt();
        String[] dimensionNames = new String[dimLength];

        for (int i = 0; i < dimLength; i++) {
            int dimensionId = data.readInt();
            dimensionNames[dimensionId] = data.readString();
        }
        _dimensionNames.addAll(Arrays.asList(dimensionNames));
        data.readInt(); // num nodes
        // System.out.println("Number of nodes : " + numNodes);
        // System.out.println(data.length());
        RandomAccessInput in = data.randomAccessSlice(data.getFilePointer(), data.length() - data.getFilePointer());
        _root = new OffHeapStarTreeNode(in, 0);
    }

    @Override
    public StarTreeNode getRoot() {
        return _root;
    }

    @Override
    public List<String> getDimensionNames() {
        return _dimensionNames;
    }

    @Override
    public void printTree(Map<String, Map<String, String>> dictionaryMap)
        throws IOException {
        printTreeHelper(dictionaryMap, _root, 0);
    }

    /** Helper method to print the tree. */
    private void printTreeHelper(Map<String, Map<String, String>> dictionaryMap, OffHeapStarTreeNode node, int level)
        throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < level; i++) {
            stringBuilder.append("  ");
        }
        String dimensionName = "ALL";
        int dimensionId = node.getDimensionId();
        if (dimensionId != StarTreeNode.ALL) {
            dimensionName = _dimensionNames.get(dimensionId);
        }
        String dimensionValueString = "ALL";
        long dimensionValue = node.getDimensionValue();
        if (dimensionValue != StarTreeNode.ALL) {
            // dimensionValueString = dictionaryMap.get(dimensionName).get(dimensionValue).toString();
        }

        // For leaf node, child dimension id is -1
        String childDimensionName = "null";
        int childDimensionId = node.getChildDimensionId();
        if (childDimensionId != -1) {
            childDimensionName = _dimensionNames.get(childDimensionId);
        }

        String formattedOutput = new StringJoiner(" - ").add("level : " + level).add("dimensionName : " + dimensionName)
            .add("dimensionValue : " + dimensionValueString).add("childDimensionName : " + childDimensionName)
            .add("startDocId : " + node.getStartDocId()).add("endDocId : " + node.getEndDocId())
            .add("aggregatedDocId : " + node.getAggregatedDocId()).add("numChildren : " + node.getNumChildren())
            .toString();

        stringBuilder.append(formattedOutput);
        // System.out.println(stringBuilder.toString());

        if (!node.isLeaf()) {
            Iterator<OffHeapStarTreeNode> childrenIterator = node.getChildrenIterator();
            while (childrenIterator.hasNext()) {
                printTreeHelper(dictionaryMap, childrenIterator.next(), level + 1);
            }
        }
    }
}

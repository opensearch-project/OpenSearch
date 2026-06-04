/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.node;

import org.opensearch.common.SetOnce;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.ALL;

/**
 * Represents a node in a tree data structure, specifically designed for a star-tree implementation.
 * A star-tree node will represent both star and non-star nodes.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class InMemoryTreeNode {

    public InMemoryTreeNode() {
        this.dimensionId = ALL;
        this.startDocId = ALL;
        this.endDocId = ALL;
        this.nodeType = (byte) 0;
        this.dimensionValue = ALL;
        this.childStarNode = new SetOnce<>();
        this.children = new LinkedHashMap<>();
    }

    public InMemoryTreeNode(int dimensionId, int startDocId, int endDocId, byte nodeType, long dimensionValue) {
        this.dimensionId = dimensionId;
        this.startDocId = startDocId;
        this.endDocId = endDocId;
        this.nodeType = nodeType;
        this.dimensionValue = dimensionValue;
        this.childStarNode = new SetOnce<>();
        this.children = new LinkedHashMap<>();
    }

    /**
     * The dimension id for the dimension (field) associated with this star-tree node.
     */
    private final int dimensionId;

    /**
     * The starting document id (inclusive) associated with this star-tree node.
     */
    private final int startDocId;

    /**
     * The ending document id (exclusive) associated with this star-tree node.
     */
    private final int endDocId;

    /**
     * The aggregated document id associated with this star-tree node.
     */
    private int aggregatedDocId = ALL;

    /**
     * The child dimension identifier associated with this star-tree node.
     */
    private int childDimensionId = ALL;

    /**
     * The value of the dimension associated with this star-tree node.
     */
    private final long dimensionValue;

    /**
     * A byte indicating whether the node is star node, null node or default node (with dimension value present).
     */
    private byte nodeType;

    /**
     * A map containing the child nodes of this star-tree node, keyed by their dimension id.
     */
    private final Map<Long, InMemoryTreeNode> children;

    /**
     * A map containing the child star node of this star-tree node.
     */
    private final SetOnce<InMemoryTreeNode> childStarNode;

    public long getDimensionValue() {
        return dimensionValue;
    }

    public byte getNodeType() {
        return nodeType;
    }

    public boolean hasChild() {
        return !(this.children.isEmpty() && this.childStarNode.get() == null);
    }

    public int getDimensionId() {
        return dimensionId;
    }

    public int getStartDocId() {
        return startDocId;
    }

    public int getEndDocId() {
        return endDocId;
    }

    public void setNodeType(byte nodeType) {
        this.nodeType = nodeType;
    }

    public void addChildNode(InMemoryTreeNode childNode, Long dimensionValue) {
        if (childNode.getNodeType() == StarTreeNodeType.STAR.getValue()) {
            this.childStarNode.set(childNode);
        } else {
            this.children.put(dimensionValue, childNode);
            assert assertStarTreeChildOrder(childNode);
        }
    }

    public Map<Long, InMemoryTreeNode> getChildren() {
        return children;
    }

    public InMemoryTreeNode getChildStarNode() {
        return childStarNode.get();
    }

    public int getChildDimensionId() {
        return childDimensionId;
    }

    public void setChildDimensionId(int childDimensionId) {
        this.childDimensionId = childDimensionId;
    }

    public int getAggregatedDocId() {
        return aggregatedDocId;
    }

    public void setAggregatedDocId(int aggregatedDocId) {
        this.aggregatedDocId = aggregatedDocId;
    }

    private boolean assertStarTreeChildOrder(InMemoryTreeNode childNode) {
        if (childNode.nodeType != StarTreeNodeType.NULL.getValue() && !this.children.isEmpty()) {
            InMemoryTreeNode lastNode = null;
            for (Map.Entry<Long, InMemoryTreeNode> entry : this.children.entrySet()) {
                lastNode = entry.getValue();
            }
            assert lastNode.dimensionValue <= childNode.dimensionValue;
        } else if (childNode.nodeType == StarTreeNodeType.NULL.getValue() && !this.children.isEmpty()) {
            InMemoryTreeNode lastNode = null;
            for (Map.Entry<Long, InMemoryTreeNode> entry : this.children.entrySet()) {
                lastNode = entry.getValue();
            }
            assert lastNode.nodeType == StarTreeNodeType.NULL.getValue();
        }
        return true;
    }

}

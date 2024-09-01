/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.node;

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
        this.children = new LinkedHashMap<>();
    }

    public InMemoryTreeNode(int dimensionId, int startDocId, int endDocId, byte nodeType, long dimensionValue) {
        this.dimensionId = dimensionId;
        this.startDocId = startDocId;
        this.endDocId = endDocId;
        this.nodeType = nodeType;
        this.dimensionValue = dimensionValue;
        this.children = new LinkedHashMap<>();
    }

    /**
     * The dimension id for the dimension (field) associated with this star-tree node.
     */
    private int dimensionId = ALL;

    /**
     * The starting document id (inclusive) associated with this star-tree node.
     */
    private int startDocId = ALL;

    /**
     * The ending document id (exclusive) associated with this star-tree node.
     */
    private int endDocId = ALL;

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
    private long dimensionValue = ALL;

    /**
     * A byte indicating whether the node is star node, null node or default node (with dimension value present).
     */
    private byte nodeType = 0;

    /**
     * A map containing the child nodes of this star-tree node, keyed by their dimension id.
     */
    private final Map<Long, InMemoryTreeNode> children;

    /**
     * A map containing the child star node of this star-tree node.
     */
    private InMemoryTreeNode childStarNode;

    public long getDimensionValue() {
        return dimensionValue;
    }

    public byte getNodeType() {
        return nodeType;
    }

    public boolean hasChild() {
        return !(this.children.isEmpty() && this.childStarNode == null);
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
            this.childStarNode = childNode;
        } else {
            this.children.put(dimensionValue, childNode);
        }
    }

    public Map<Long, InMemoryTreeNode> getChildren() {
        return children;
    }

    public InMemoryTreeNode getChildStarNode() {
        return childStarNode;
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
}

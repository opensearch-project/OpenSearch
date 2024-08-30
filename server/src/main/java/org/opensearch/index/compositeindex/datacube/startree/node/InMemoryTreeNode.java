/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.node;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;

import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.ALL;

/**
 * /**
 * Represents a node in a tree data structure, specifically designed for a star-tree implementation.
 * A star-tree node will represent both star and non-star nodes.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class InMemoryTreeNode {

    /**
     * The dimension id for the dimension (field) associated with this star-tree node.
     */
    public int dimensionId = ALL;

    /**
     * The starting document id (inclusive) associated with this star-tree node.
     */
    public int startDocId = ALL;

    /**
     * The ending document id (exclusive) associated with this star-tree node.
     */
    public int endDocId = ALL;

    /**
     * The aggregated document id associated with this star-tree node.
     */
    public int aggregatedDocId = ALL;

    /**
     * The child dimension identifier associated with this star-tree node.
     */
    public int childDimensionId = ALL;

    /**
     * The value of the dimension associated with this star-tree node.
     */
    public long dimensionValue = ALL;

    /**
     * A byte indicating whether the node is star node, null node or default node (with dimension value present).
     */
    public byte nodeType = 0;

    /**
     * A map containing the child nodes of this star-tree node, keyed by their dimension id.
     */
    public Map<Long, InMemoryTreeNode> children;

    public long getDimensionValue() {
        return dimensionValue;
    }

    public byte getNodeType() {
        return nodeType;
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;

/**
 * /**
 * Represents a node in a tree data structure, specifically designed for a star-tree implementation.
 * A star-tree node will represent both star and non-star nodes.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TreeNode {

    public static final int ALL = -1;

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
     * A flag indicating whether this node is a star node (a node that represents an aggregation of all dimensions).
     */
    public boolean isStarNode = false;

    /**
     * A map containing the child nodes of this star-tree node, keyed by their dimension id.
     */
    public Map<Long, TreeNode> children;

    public long getDimensionValue() {
        return dimensionValue;
    }
}

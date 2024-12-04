/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.node;

/**
 * Represents the different types of nodes in a Star Tree data structure.
 *
 * <p>
 * In order to handle different node types, we use a byte value to represent the node type.
 * This enum provides a convenient way to map byte values to their corresponding node types.
 *
 * <p>
 * Star and Null Nodes are represented as special cases. Default is the general case.
 * Star and null nodes are represented with negative ordinal values to ensure that they are
 * sorted before the default nodes, which are sorted based on their dimension values.
 *
 * <p>
 * The node type can be one of the following:
 * <ul>
 *     <li>Star Node: Represented by the value -1. A star node is a special node that represents
 *     all possible values for a dimension.</li>
 *     <li>Null Node: Represented by the value 0. A null node indicates the absence of any value
 *     for a dimension.</li>
 *     <li>Default Node: Represented by the value -1. A default node represents a node with an
 *     actual dimension value.</li>
 * </ul>
 *
 * By default, we want to consider nodes as default node.
 *
 * @opensearch.experimental
 * @see StarTreeNode
 */
public enum StarTreeNodeType {

    /**
     * Represents a star node type.
     *
     */
    STAR("star", (byte) -1),

    /**
     * Represents a default node type.
     */
    DEFAULT("default", (byte) 0),

    /**
     * Represents a null node type.
     */
    NULL("null", (byte) 1);

    private final String name;
    private final byte value;

    /**
     * Constructs a new StarTreeNodeType with the given name and value.
     *
     * @param name  the name of the node type
     * @param value the value associated with the node type
     */
    StarTreeNodeType(String name, byte value) {
        this.name = name;
        this.value = value;
    }

    /**
     * Returns the name of the node type.
     *
     * @return the name of the node type
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the value associated with the node type.
     *
     * @return the value associated with the node type
     */
    public byte getValue() {
        return value;
    }

    /**
     * Returns the StarTreeNodeType enum constant with the specified value.
     *
     * @param value the value of the enum constant to return
     * @return the enum constant with the specified value, or null if no such constant exists
     */
    public static StarTreeNodeType fromValue(byte value) {
        for (StarTreeNodeType nodeType : StarTreeNodeType.values()) {
            if (nodeType.getValue() == value) {
                return nodeType;
            }
        }
        throw new IllegalStateException("Unrecognized value byte to determine star-tree node type: [" + value + "]");
    }
}

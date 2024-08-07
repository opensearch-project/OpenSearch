/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.node;

/**
 * Represents the different types of nodes in a StarTree data structure.
 * <p>
 * In order to handle different node types, we use a byte value to represent the node type.
 * This enum provides a convenient way to map byte values to their corresponding node types.
 * <p>
 * Star and Null Nodes are represented as special cases. Default is the general case.
 * Star and Null nodes are represented with negative ordinals so that first node is Star, second node is Null Node
 * and the rest of the default nodes are sorted based on dimension values.
 * <p>
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
    STAR("star", (byte) -2),

    /**
     * Represents a null node type.
     */
    NULL("null", (byte) -1),

    /**
     * Represents a default node type.
     */
    DEFAULT("default", (byte) 0);

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
        return null;
    }
}

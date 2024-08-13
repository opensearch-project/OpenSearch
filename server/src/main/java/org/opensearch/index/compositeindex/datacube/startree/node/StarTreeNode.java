/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.node;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.Iterator;

/**
 * Interface that represents star tree node
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface StarTreeNode {

    /**
     * Returns the dimension ID of the current star-tree node.
     *
     * @return the dimension ID
     * @throws IOException if an I/O error occurs while reading the dimension ID
     */
    int getDimensionId() throws IOException;

    /**
     * Returns the dimension value of the current star-tree node.
     *
     * @return the dimension value
     * @throws IOException if an I/O error occurs while reading the dimension value
     */
    long getDimensionValue() throws IOException;

    /**
     * Returns the dimension ID of the child star-tree node.
     *
     * @return the child dimension ID
     * @throws IOException if an I/O error occurs while reading the child dimension ID
     */
    int getChildDimensionId() throws IOException;

    /**
     * Returns the start document ID of the current star-tree node.
     *
     * @return the start document ID
     * @throws IOException if an I/O error occurs while reading the start document ID
     */
    int getStartDocId() throws IOException;

    /**
     * Returns the end document ID of the current star-tree node.
     *
     * @return the end document ID
     * @throws IOException if an I/O error occurs while reading the end document ID
     */
    int getEndDocId() throws IOException;

    /**
     * Returns the aggregated document ID of the current star-tree node.
     *
     * @return the aggregated document ID
     * @throws IOException if an I/O error occurs while reading the aggregated document ID
     */
    int getAggregatedDocId() throws IOException;

    /**
     * Returns the number of children of the current star-tree node.
     *
     * @return the number of children
     * @throws IOException if an I/O error occurs while reading the number of children
     */
    int getNumChildren() throws IOException;

    /**
     * Checks if the current node is a leaf star-tree node.
     *
     * @return true if the node is a leaf node, false otherwise
     */
    boolean isLeaf();

    /**
     * Determines the type of the current node in the Star Tree index structure.
     *
     * <p>The node type can be one of the following:
     * <ul>
     *     <li>Star Node: Represented by the value -2.
     *     <li>Null Node: Represented by the value -1.
     *     <li>Default Node: Represented by the value 0.
     * </ul>
     * @see StarTreeNodeType
     *
     * @return The type of the current node, represented by the corresponding integer value (-2, -1, or 0).
     * @throws IOException if an I/O error occurs while reading the node type
     */
    byte getStarTreeNodeType() throws IOException;

    /**
     * Returns the child star-tree node for the given dimension value.
     *
     * @param dimensionValue the dimension value
     * @return the child node for the given dimension value or null if child is not present
     * @throws IOException if an I/O error occurs while retrieving the child node
     */
    StarTreeNode getChildForDimensionValue(long dimensionValue, boolean isStar) throws IOException;

    /**
     * Returns an iterator over the children of the current star-tree node.
     *
     * @return an iterator over the children
     * @throws IOException if an I/O error occurs while retrieving the children iterator
     */
    Iterator<? extends StarTreeNode> getChildrenIterator() throws IOException;
}

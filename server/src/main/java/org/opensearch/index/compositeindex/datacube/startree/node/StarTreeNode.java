/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.node;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.startree.StarTreeNodeCollector;

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
     *     <li>Star Node: Represented by the value -1.
     *     <li>Null Node: Represented by the value 1.
     *     <li>Default Node: Represented by the value 0.
     * </ul>
     * @see StarTreeNodeType
     *
     * @return The type of the current node, represented by the corresponding integer value (-1, 1, 0).
     * @throws IOException if an I/O error occurs while reading the node type
     */
    byte getStarTreeNodeType() throws IOException;

    /**
     * Returns the child node for the given dimension value in the star-tree.
     *
     * @param dimensionValue  the dimension value
     * @return the child node for the given dimension value or null if child is not present
     * @throws IOException if an I/O error occurs while retrieving the child node
     */
    default StarTreeNode getChildForDimensionValue(Long dimensionValue) throws IOException {
        return getChildForDimensionValue(dimensionValue, null);
    }

    /**
     * Matches the given @dimensionValue amongst the child default nodes for this node.
     * @param dimensionValue : Value to match
     * @param lastMatchedChild : If not null, binary search will use this as the start/low
     * @return : Matched StarTreeNode or null if not found
     * @throws IOException : Any exception in reading the node data from index.
     */
    StarTreeNode getChildForDimensionValue(Long dimensionValue, StarTreeNode lastMatchedChild) throws IOException;

    /**
     * Collects all matching child nodes whose dimension values lie within the range of low and high, both inclusive.
     * @param low : Starting of the range ( inclusive )
     * @param high : End of the range ( inclusive )
     * @param collector : Collector to collect the matched child StarTreeNode's
     * @throws IOException : Any exception in reading the node data from index.
     */
    void collectChildrenInRange(long low, long high, StarTreeNodeCollector collector) throws IOException;

    /**
     * Returns the child star node for a node in the star-tree.
     *
     * @return the child node for the star node if star child node is not present
     * @throws IOException if an I/O error occurs while retrieving the child node
     */
    StarTreeNode getChildStarNode() throws IOException;

    /**
     * Returns an iterator over the children of the current star-tree node.
     *
     * @return an iterator over the children
     * @throws IOException if an I/O error occurs while retrieving the children iterator
     */
    Iterator<? extends StarTreeNode> getChildrenIterator() throws IOException;
}

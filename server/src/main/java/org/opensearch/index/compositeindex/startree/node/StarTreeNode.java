/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.startree.node;

import java.io.IOException;
import java.util.Iterator;

/**
 * Class representing each node in star tree
 * @opensearch.experimental
 */
public interface StarTreeNode {
    long ALL = -1l;

    /** Get the index of the dimension. */
    int getDimensionId() throws IOException;

    /** Get the value (dictionary id) of the dimension. */
    long getDimensionValue() throws IOException;

    /** Get the child dimension id. */
    int getChildDimensionId() throws IOException;

    /** Get the index of the start document. */
    int getStartDocId() throws IOException;

    /** Get the index of the end document (exclusive). */
    int getEndDocId() throws IOException;

    /** Get the index of the aggregated document. */
    int getAggregatedDocId() throws IOException;

    /** Get the number of children nodes. */
    int getNumChildren() throws IOException;

    /** Return true if the node is a leaf node, false otherwise. */
    boolean isLeaf();

    /**
     * Get the child node corresponding to the given dimension value (dictionary id), or null if such
     * child does not exist.
     */
    StarTreeNode getChildForDimensionValue(long dimensionValue) throws IOException;

    /** Get the iterator over all children nodes. */
    Iterator<? extends StarTreeNode> getChildrenIterator() throws IOException;
}

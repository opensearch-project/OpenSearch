/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.startree.node;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface for star tree
 * @opensearch.experimental
 */
public interface StarTree {

    /** Get the root node of the star tree. */
    StarTreeNode getRoot();

    /**
     * Get a list of all dimension names. The node dimension id is the index of the dimension name in
     * this list.
     */
    List<String> getDimensionNames();

    /** Prints the tree for its visual representation. */
    void printTree(Map<String, Map<String, String>> dictionaryMap) throws IOException;
}

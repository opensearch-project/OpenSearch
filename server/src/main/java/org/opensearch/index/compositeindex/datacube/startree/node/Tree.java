/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.node;

/**
 * Interface for star-tree.
 *
 * @opensearch.experimental
 */
public interface Tree {

    /**
     * Fetches the root node of the star-tree.
     * @return the root of the star-tree
     */
    StarTreeNode getRoot();

}

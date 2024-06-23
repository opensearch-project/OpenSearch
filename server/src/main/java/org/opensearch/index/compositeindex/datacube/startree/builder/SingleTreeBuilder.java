/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import java.io.Closeable;
import java.io.IOException;

/**
 * A star-tree builder that builds a single star-tree.
 * @opensearch.experimental
 */
public interface SingleTreeBuilder extends Closeable {

    /**
     * Builds the star tree based on star-tree field
     * @throws IOException when we are unable to build star-tree
     */
    void build() throws Exception;
}

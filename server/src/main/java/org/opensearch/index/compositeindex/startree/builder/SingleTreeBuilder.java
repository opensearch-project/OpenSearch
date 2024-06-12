/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.startree.builder;

import org.opensearch.index.compositeindex.startree.data.StarTreeDocValues;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * A star-tree builder that builds a single star-tree.
 * @opensearch.experimental
 */
public interface SingleTreeBuilder extends Closeable {

    /**
     * Builds the data structure for the given composite index config.
     */
    void build() throws Exception;

    /**
     * Builds the star-tree during segment merges
     */
    void build(List<StarTreeDocValues> starTreeDocValues) throws IOException;

}

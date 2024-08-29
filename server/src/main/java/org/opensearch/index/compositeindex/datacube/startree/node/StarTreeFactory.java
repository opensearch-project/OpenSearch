/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.node;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.StarTreeMetadata;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.node.FixedLengthStarTreeNode;

import java.io.IOException;

/**
 * A factory class for creating off-heap implementations of star-tree nodes.
 *
 * <p>This class provides a static factory method to create instances of {@link StarTreeNode}
 * from an {@link IndexInput} and {@link StarTreeMetadata}. The implementation uses an
 * off-heap data structure to store and access the star-tree data efficiently using random access.
 *
 * @opensearch.experimental
 */
public class StarTreeFactory {

    /**
     * Creates a new instance of {@link StarTreeNode} from the provided {@link IndexInput} and
     * {@link StarTreeMetadata}.
     *
     * @param data The {@link IndexInput} containing the star-tree data.
     * @param starTreeMetadata The {@link StarTreeMetadata} containing metadata about the star-tree.
     * @return A new instance of {@link StarTreeNode} representing the root of the star-tree.
     * @throws IOException If an error occurs while reading the star-tree data.
     */
    public static StarTreeNode createStarTree(IndexInput data, StarTreeMetadata starTreeMetadata) throws IOException {
        RandomAccessInput in = data.randomAccessSlice(0, starTreeMetadata.getDataLength());
        return new FixedLengthStarTreeNode(in, 0);
    }

}

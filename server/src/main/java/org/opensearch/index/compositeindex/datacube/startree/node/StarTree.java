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

import java.io.IOException;

/**
 * Off heap implementation of the star-tree.
 *
 * @opensearch.experimental
 */
public class StarTree {
    private final FixedLengthStarTreeNode root;

    public StarTree(IndexInput data, StarTreeMetadata starTreeMetadata) throws IOException {
        RandomAccessInput in = data.randomAccessSlice(0, starTreeMetadata.getDataLength());
        root = new FixedLengthStarTreeNode(in, 0);
    }

    public StarTreeNode getRoot() {
        return root;
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.data.StarTreeDataWriter;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.StarTreeMetadata;

import java.io.IOException;

import static org.opensearch.index.compositeindex.CompositeIndexConstants.COMPOSITE_FIELD_MARKER;
import static org.opensearch.index.compositeindex.datacube.startree.fileformats.StarTreeWriter.VERSION_CURRENT;

/**
 * Off heap implementation of the star-tree.
 *
 * @opensearch.experimental
 */
public class StarTree {
    private static final Logger logger = LogManager.getLogger(StarTree.class);
    private final FixedLengthStarTreeNode root;
    private final Integer numNodes;

    public StarTree(IndexInput data, StarTreeMetadata starTreeMetadata) throws IOException {
        long magicMarker = data.readLong();
        if (COMPOSITE_FIELD_MARKER != magicMarker) {
            logger.error("Invalid magic marker");
            throw new IOException("Invalid magic marker");
        }
        int version = data.readInt();
        if (VERSION_CURRENT != version) {
            logger.error("Invalid star tree version");
            throw new IOException("Invalid version");
        }
        numNodes = data.readInt(); // num nodes

        RandomAccessInput in = data.randomAccessSlice(
            StarTreeDataWriter.computeStarTreeDataHeaderByteSize(),
            starTreeMetadata.getDataLength() - StarTreeDataWriter.computeStarTreeDataHeaderByteSize()
        );
        root = new FixedLengthStarTreeNode(in, 0);
    }

    public StarTreeNode getRoot() {
        return root;
    }

    /**
     * Returns the number of nodes in star-tree
     *
     * @return number of nodes in te star-tree
     */
    public Integer getNumNodes() {
        return numNodes;
    }

}

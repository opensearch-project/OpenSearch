/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.fileformats;

import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.data.StarTreeDataWriter;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.StarTreeMetaWriter;
import org.opensearch.index.compositeindex.datacube.startree.node.InMemoryTreeNode;

import java.io.IOException;
import java.util.List;

/**
 * Util class for building star tree
 *
 * @opensearch.experimental
 */
public class StarTreeWriter {

    /** Initial version for the star tree writer */
    public static final int VERSION_START = 0;

    /** Current version for the star tree writer */
    public static final int VERSION_CURRENT = VERSION_START;

    public StarTreeWriter() {}

    /**
     * Write star tree to index output stream
     *
     * @param dataOut  data index output
     * @param rootNode root star-tree node
     * @param numNodes number of nodes in the star tree
     * @param name     name of the star-tree field
     * @return total size of the three
     * @throws IOException when star-tree data serialization fails
     */
    public long writeStarTree(IndexOutput dataOut, InMemoryTreeNode rootNode, int numNodes, String name) throws IOException {
        return StarTreeDataWriter.writeStarTree(dataOut, rootNode, numNodes, name);
    }

    /**
     * Write star tree metadata to index output stream
     *
     * @param metaOut                meta index output
     * @param starTreeField          star tree field
     * @param metricAggregatorInfos  metric aggregator infos
     * @param numNodes               number of nodes in the star tree
     * @param segmentAggregatedCount segment aggregated count
     * @param dataFilePointer        data file pointer
     * @param dataFileLength         data file length
     * @throws IOException when star-tree data serialization fails
     */
    public void writeStarTreeMetadata(
        IndexOutput metaOut,
        StarTreeField starTreeField,
        List<MetricAggregatorInfo> metricAggregatorInfos,
        Integer numNodes,
        Integer segmentAggregatedCount,
        long dataFilePointer,
        long dataFileLength
    ) throws IOException {
        StarTreeMetaWriter.writeStarTreeMetadata(
            metaOut,
            starTreeField,
            metricAggregatorInfos,
            numNodes,
            segmentAggregatedCount,
            dataFilePointer,
            dataFileLength
        );
    }

}

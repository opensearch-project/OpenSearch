/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.fileformats.meta;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo;
import org.opensearch.index.mapper.CompositeMappedFieldType;

import java.io.IOException;
import java.util.List;

import static org.opensearch.index.compositeindex.CompositeIndexConstants.COMPOSITE_FIELD_MARKER;
import static org.opensearch.index.compositeindex.datacube.startree.fileformats.StarTreeWriter.VERSION_CURRENT;

/**
 * The utility class for serializing the metadata of a star-tree data structure.
 * The metadata includes information about the dimensions, metrics, and other relevant details
 * related to the star tree.
 *
 * @opensearch.experimental
 */
public class StarTreeMetaWriter {

    private static final Logger logger = LogManager.getLogger(StarTreeMetaWriter.class);

    /**
     * Writes the star-tree metadata.
     *
     * @param metaOut                the IndexOutput to write the metadata
     * @param starTreeField          the star-tree field
     * @param metricAggregatorInfos  the list of metric aggregator information
     * @param segmentAggregatedCount the aggregated document count for the segment
     * @param numNodes               number of nodes in the star tree
     * @param dataFilePointer        the file pointer to the start of the star tree data
     * @param dataFileLength         the length of the star tree data file
     * @throws IOException if an I/O error occurs while serializing the metadata
     */
    public static void writeStarTreeMetadata(
        IndexOutput metaOut,
        StarTreeField starTreeField,
        List<MetricAggregatorInfo> metricAggregatorInfos,
        Integer numNodes,
        Integer segmentAggregatedCount,
        Integer numStarTreeDocs,
        long dataFilePointer,
        long dataFileLength
    ) throws IOException {

        long initialMetaFilePointer = metaOut.getFilePointer();

        writeMetaHeader(metaOut);

        // TODO: Replace the parameters with StarTreeMetadata class object
        writeMeta(
            metaOut,
            metricAggregatorInfos,
            starTreeField,
            numNodes,
            segmentAggregatedCount,
            numStarTreeDocs,
            dataFilePointer,
            dataFileLength
        );

        logger.debug(
            "Star tree meta size in bytes : {} for star-tree field {}",
            metaOut.getFilePointer() - initialMetaFilePointer,
            starTreeField.getName()
        );
    }

    /**
     * Writes the star-tree metadata header.
     *
     * @param metaOut            the IndexOutput to write the header
     * @throws IOException if an I/O error occurs while writing the header
     */
    private static void writeMetaHeader(IndexOutput metaOut) throws IOException {
        // magic marker for sanity
        metaOut.writeLong(COMPOSITE_FIELD_MARKER);

        // version
        metaOut.writeVInt(VERSION_CURRENT);

    }

    /**
     * Writes the star-tree metadata.
     *
     * @param metaOut                   the IndexOutput to write the metadata
     * @param metricAggregatorInfos     the list of metric aggregator information
     * @param starTreeField             the star tree field
     * @param numNodes                  number of nodes in the star tree
     * @param segmentAggregatedDocCount the aggregated document count for the segment
     * @param numStarTreeDocs           the total number of star tree documents for the segment
     * @param dataFilePointer           the file pointer to the start of the star-tree data
     * @param dataFileLength            the length of the star-tree data file
     * @throws IOException if an I/O error occurs while writing the metadata
     */
    private static void writeMeta(
        IndexOutput metaOut,
        List<MetricAggregatorInfo> metricAggregatorInfos,
        StarTreeField starTreeField,
        int numNodes,
        Integer segmentAggregatedDocCount,
        Integer numStarTreeDocs,
        long dataFilePointer,
        long dataFileLength
    ) throws IOException {

        // star tree field name
        metaOut.writeString(starTreeField.getName());

        // star tree field type
        metaOut.writeString(CompositeMappedFieldType.CompositeFieldType.STAR_TREE.getName());

        // number of nodes
        metaOut.writeInt(numNodes);

        // number of dimensions
        // TODO: Revisit the number of dimensions for timestamps (as we will split timestamp into min, hour, etc.)
        metaOut.writeVInt(starTreeField.getDimensionsOrder().size());

        // dimensions
        // TODO: Add sub-dimensions for timestamps (as we will split timestamp into min, hour, etc.)
        for (Dimension dimension : starTreeField.getDimensionsOrder()) {
            metaOut.writeString(dimension.getField());
        }

        // number of metrics
        metaOut.writeVInt(metricAggregatorInfos.size());

        // metric - metric stat pair
        for (MetricAggregatorInfo metricAggregatorInfo : metricAggregatorInfos) {
            metaOut.writeString(metricAggregatorInfo.getField());
            int metricStatOrdinal = metricAggregatorInfo.getMetricStat().getMetricOrdinal();
            metaOut.writeVInt(metricStatOrdinal);
        }

        // segment aggregated document count
        metaOut.writeVInt(segmentAggregatedDocCount);

        // segment star tree document count
        metaOut.writeVInt(numStarTreeDocs);

        // max leaf docs
        metaOut.writeVInt(starTreeField.getStarTreeConfig().maxLeafDocs());

        // number of skip star node creation dimensions
        metaOut.writeVInt(starTreeField.getStarTreeConfig().getSkipStarNodeCreationInDims().size());

        // skip star node creations
        for (String dimension : starTreeField.getStarTreeConfig().getSkipStarNodeCreationInDims()) {
            metaOut.writeString(dimension);
        }

        // star tree build-mode
        metaOut.writeByte(starTreeField.getStarTreeConfig().getBuildMode().getBuildModeOrdinal());

        // star-tree data file pointer
        metaOut.writeVLong(dataFilePointer);

        // star-tree data file length
        metaOut.writeVLong(dataFileLength);

    }
}

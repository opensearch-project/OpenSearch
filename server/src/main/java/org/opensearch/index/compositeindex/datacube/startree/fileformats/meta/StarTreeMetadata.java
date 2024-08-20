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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.compositeindex.CompositeIndexMetadata;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.mapper.CompositeMappedFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Holds the associated metadata for the building of star-tree.
 *
 * @opensearch.experimental
 */
public class StarTreeMetadata extends CompositeIndexMetadata {
    private static final Logger logger = LogManager.getLogger(StarTreeMetadata.class);
    private final IndexInput meta;
    private final String starTreeFieldName;
    private final String starTreeFieldType;
    private final List<String> dimensionFields;
    private final List<MetricEntry> metricEntries;
    private final Integer segmentAggregatedDocCount;
    private final Integer starTreeDocCount;
    private final Integer maxLeafDocs;
    private final Set<String> skipStarNodeCreationInDims;
    private final StarTreeFieldConfiguration.StarTreeBuildMode starTreeBuildMode;
    private final long dataStartFilePointer;
    private final long dataLength;

    /**
     * A star tree metadata constructor to initialize star tree metadata from the segment file (.cim) using index input.
     *
     * @param metaIn an index input to read star-tree meta
     * @param compositeFieldName name of the composite field. Here, name of the star-tree field.
     * @param compositeFieldType type of the composite field. Here, STAR_TREE field.
     * @throws IOException if unable to read star-tree metadata from the file
     */
    public StarTreeMetadata(IndexInput metaIn, String compositeFieldName, CompositeMappedFieldType.CompositeFieldType compositeFieldType)
        throws IOException {
        super(compositeFieldName, compositeFieldType);
        this.meta = metaIn;
        try {
            this.starTreeFieldName = this.getCompositeFieldName();
            this.starTreeFieldType = this.getCompositeFieldType().getName();
            this.dimensionFields = readStarTreeDimensions();
            this.metricEntries = readMetricEntries();
            this.segmentAggregatedDocCount = readSegmentAggregatedDocCount();
            this.starTreeDocCount = readStarTreeDocCount();
            this.maxLeafDocs = readMaxLeafDocs();
            this.skipStarNodeCreationInDims = readSkipStarNodeCreationInDims();
            this.starTreeBuildMode = readBuildMode();
            this.dataStartFilePointer = readDataStartFilePointer();
            this.dataLength = readDataLength();
        } catch (Exception e) {
            logger.error("Unable to read star-tree metadata from the file");
            throw new CorruptIndexException("Unable to read star-tree metadata from the file", metaIn);
        }
    }

    /**
     * A star tree metadata constructor to initialize star tree metadata.
     * Used for testing.
     *
     * @param meta an index input to read star-tree meta
     * @param compositeFieldName name of the composite field. Here, name of the star-tree field.
     * @param compositeFieldType type of the composite field. Here, STAR_TREE field.
     * @param dimensionFields list of dimension fields
     * @param metricEntries list of metric entries
     * @param segmentAggregatedDocCount segment aggregated doc count
     * @param starTreeDocCount        the total number of star tree documents for the segment
     * @param maxLeafDocs max leaf docs
     * @param skipStarNodeCreationInDims set of dimensions to skip star node creation
     * @param starTreeBuildMode star tree build mode
     * @param dataStartFilePointer data start file pointer
     * @param dataLength data length
     */
    public StarTreeMetadata(
        String compositeFieldName,
        CompositeMappedFieldType.CompositeFieldType compositeFieldType,
        IndexInput meta,
        List<String> dimensionFields,
        List<MetricEntry> metricEntries,
        Integer segmentAggregatedDocCount,
        Integer starTreeDocCount,
        Integer maxLeafDocs,
        Set<String> skipStarNodeCreationInDims,
        StarTreeFieldConfiguration.StarTreeBuildMode starTreeBuildMode,
        long dataStartFilePointer,
        long dataLength
    ) {
        super(compositeFieldName, compositeFieldType);
        this.meta = meta;
        this.starTreeFieldName = compositeFieldName;
        this.starTreeFieldType = compositeFieldType.getName();
        this.dimensionFields = dimensionFields;
        this.metricEntries = metricEntries;
        this.segmentAggregatedDocCount = segmentAggregatedDocCount;
        this.starTreeDocCount = starTreeDocCount;
        this.maxLeafDocs = maxLeafDocs;
        this.skipStarNodeCreationInDims = skipStarNodeCreationInDims;
        this.starTreeBuildMode = starTreeBuildMode;
        this.dataStartFilePointer = dataStartFilePointer;
        this.dataLength = dataLength;
    }

    private int readDimensionsCount() throws IOException {
        return meta.readVInt();
    }

    private List<String> readStarTreeDimensions() throws IOException {
        int dimensionCount = readDimensionsCount();
        List<String> dimensionFields = new ArrayList<>();

        for (int i = 0; i < dimensionCount; i++) {
            dimensionFields.add(meta.readString());
        }

        return dimensionFields;
    }

    private int readMetricsCount() throws IOException {
        return meta.readVInt();
    }

    private List<MetricEntry> readMetricEntries() throws IOException {
        int metricCount = readMetricsCount();
        List<MetricEntry> metricEntries = new ArrayList<>();

        for (int i = 0; i < metricCount; i++) {
            String metricFieldName = meta.readString();
            int metricStatOrdinal = meta.readVInt();
            metricEntries.add(new MetricEntry(metricFieldName, MetricStat.fromMetricOrdinal(metricStatOrdinal)));
        }

        return metricEntries;
    }

    private int readSegmentAggregatedDocCount() throws IOException {
        return meta.readVInt();
    }

    private Integer readStarTreeDocCount() throws IOException {
        return meta.readVInt();
    }

    private int readMaxLeafDocs() throws IOException {
        return meta.readVInt();
    }

    private int readSkipStarNodeCreationInDimsCount() throws IOException {
        return meta.readVInt();
    }

    private Set<String> readSkipStarNodeCreationInDims() throws IOException {

        int skipStarNodeCreationInDimsCount = readSkipStarNodeCreationInDimsCount();
        Set<String> skipStarNodeCreationInDims = new HashSet<>();
        for (int i = 0; i < skipStarNodeCreationInDimsCount; i++) {
            skipStarNodeCreationInDims.add(meta.readString());
        }
        return skipStarNodeCreationInDims;
    }

    private StarTreeFieldConfiguration.StarTreeBuildMode readBuildMode() throws IOException {
        return StarTreeFieldConfiguration.StarTreeBuildMode.fromBuildModeOrdinal(meta.readByte());
    }

    private long readDataStartFilePointer() throws IOException {
        return meta.readVLong();
    }

    private long readDataLength() throws IOException {
        return meta.readVLong();
    }

    /**
     * Returns the name of the star-tree field.
     *
     * @return star-tree field name
     */
    public String getStarTreeFieldName() {
        return starTreeFieldName;
    }

    /**
     * Returns the type of the star tree field.
     *
     * @return star-tree field type
     */
    public String getStarTreeFieldType() {
        return starTreeFieldType;
    }

    /**
     * Returns the list of dimension field numbers.
     *
     * @return star-tree dimension field numbers
     */
    public List<String> getDimensionFields() {
        return dimensionFields;
    }

    /**
     * Returns the list of metric entries.
     *
     * @return star-tree metric entries
     */
    public List<MetricEntry> getMetricEntries() {
        return metricEntries;
    }

    /**
     * Returns the aggregated document count for the star-tree.
     *
     * @return the aggregated document count for the star-tree.
     */
    public Integer getSegmentAggregatedDocCount() {
        return segmentAggregatedDocCount;
    }

    /**
     * Returns the total number of star tree documents in the segment
     *
     * @return the number of star tree documents in the segment
     */
    public Integer getStarTreeDocCount() {
        return starTreeDocCount;
    }

    /**
     * Returns the max leaf docs for the star-tree.
     *
     * @return the max leaf docs.
     */
    public Integer getMaxLeafDocs() {
        return maxLeafDocs;
    }

    /**
     * Returns the set of dimensions for which star node will not be created in the star-tree.
     *
     * @return the set of dimensions.
     */
    public Set<String> getSkipStarNodeCreationInDims() {
        return skipStarNodeCreationInDims;
    }

    /**
     * Returns the build mode for the star-tree.
     *
     * @return the star-tree build mode.
     */
    public StarTreeFieldConfiguration.StarTreeBuildMode getStarTreeBuildMode() {
        return starTreeBuildMode;
    }

    /**
     * Returns the file pointer to the start of the star-tree data.
     *
     * @return start file pointer for star-tree data
     */
    public long getDataStartFilePointer() {
        return dataStartFilePointer;
    }

    /**
     * Returns the length of star-tree data
     *
     * @return star-tree length
     */
    public long getDataLength() {
        return dataLength;
    }
}

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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.CompositeIndexMetadata;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.mapper.CompositeMappedFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Holds the associated metadata for the building of star-tree.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeMetadata extends CompositeIndexMetadata {
    private static final Logger logger = LogManager.getLogger(StarTreeMetadata.class);

    /**
     * The index input for reading metadata from the segment file.
     */
    private final IndexInput meta;

    /**
     * The version of the star tree stored in the segments.
     */
    private final int version;

    /**
     * The number of the nodes in the respective star tree
     */
    private final int numberOfNodes;

    /**
     * The name of the star-tree field, used to identify the star-tree.
     */
    private final String starTreeFieldName;

    /**
     * The type of the star-tree field, indicating the specific implementation or version.
     * Here, STAR_TREE field.
     */
    private final String starTreeFieldType;

    /**
     * List of dimension fields used in the star-tree.
     */
    private final List<String> dimensionFields;

    /**
     * List of metrics, containing field names and associated metric statistics.
     */
    private final List<Metric> metrics;

    /**
     * The total number of documents aggregated in this star-tree segment.
     */
    private final int segmentAggregatedDocCount;

    /**
     * The maximum number of documents allowed in a leaf node.
     */
    private final int maxLeafDocs;

    /**
     * Set of dimensions for which star node creation should be skipped.
     */
    private final Set<String> skipStarNodeCreationInDims;

    /**
     * The build mode used for constructing the star-tree.
     */
    private final StarTreeFieldConfiguration.StarTreeBuildMode starTreeBuildMode;

    /**
     * The file pointer to the start of the associated star-tree data in the (.cid) file
     */
    private final long dataStartFilePointer;

    /**
     * The length of the star-tree data in bytes, used for reading the correct amount of data from (.cid) file
     */
    private final long dataLength;

    /**
     * The number of star tree documents in the star tree.
     */
    private final int starTreeDocCount;

    /**
     * A star tree metadata constructor to initialize star tree metadata from the segment file (.cim) using index input.
     *
     * @param metaIn             an index input to read star-tree meta
     * @param compositeFieldName name of the composite field. Here, name of the star-tree field.
     * @param compositeFieldType type of the composite field. Here, STAR_TREE field.
     * @param version The version of the star tree stored in the segments.
     * @throws IOException if unable to read star-tree metadata from the file
     */
    public StarTreeMetadata(
        IndexInput metaIn,
        String compositeFieldName,
        CompositeMappedFieldType.CompositeFieldType compositeFieldType,
        Integer version
    ) throws IOException {
        super(compositeFieldName, compositeFieldType);
        this.meta = metaIn;
        try {
            this.starTreeFieldName = this.getCompositeFieldName();
            this.starTreeFieldType = this.getCompositeFieldType().getName();
            this.version = version;
            this.numberOfNodes = readNumberOfNodes();
            this.dimensionFields = readStarTreeDimensions();
            this.metrics = readMetricEntries();
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
     * @param meta                       an index input to read star-tree meta
     * @param compositeFieldName         name of the composite field. Here, name of the star-tree field.
     * @param compositeFieldType         type of the composite field. Here, STAR_TREE field.
     * @param version The version of the star tree stored in the segments.
     * @param dimensionFields            list of dimension fields
     * @param metrics              list of metric entries
     * @param segmentAggregatedDocCount  segment aggregated doc count
     * @param starTreeDocCount        the total number of star tree documents for the segment
     * @param maxLeafDocs                max leaf docs
     * @param skipStarNodeCreationInDims set of dimensions to skip star node creation
     * @param starTreeBuildMode          star tree build mode
     * @param dataStartFilePointer       star file pointer to the associated star tree data in (.cid) file
     * @param dataLength                 length of the corresponding star-tree data in (.cid) file
     */
    public StarTreeMetadata(
        String compositeFieldName,
        CompositeMappedFieldType.CompositeFieldType compositeFieldType,
        IndexInput meta,
        Integer version,
        Integer numberOfNodes,
        List<String> dimensionFields,
        List<Metric> metrics,
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
        this.version = version;
        this.numberOfNodes = numberOfNodes;
        this.dimensionFields = dimensionFields;
        this.metrics = metrics;
        this.segmentAggregatedDocCount = segmentAggregatedDocCount;
        this.starTreeDocCount = starTreeDocCount;
        this.maxLeafDocs = maxLeafDocs;
        this.skipStarNodeCreationInDims = skipStarNodeCreationInDims;
        this.starTreeBuildMode = starTreeBuildMode;
        this.dataStartFilePointer = dataStartFilePointer;
        this.dataLength = dataLength;
    }

    private int readNumberOfNodes() throws IOException {
        return meta.readVInt();
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

    private List<Metric> readMetricEntries() throws IOException {
        int metricCount = readMetricsCount();

        Map<String, List<MetricStat>> starTreeMetricStatMap = new LinkedHashMap<>();
        for (int i = 0; i < metricCount; i++) {
            String metricName = meta.readString();
            int metricStatOrdinal = meta.readVInt();
            MetricStat metricStat = MetricStat.fromMetricOrdinal(metricStatOrdinal);
            List<MetricStat> metricStats = starTreeMetricStatMap.computeIfAbsent(metricName, field -> new ArrayList<>());
            metricStats.add(metricStat);
        }
        List<Metric> starTreeMetricMap = new ArrayList<>();
        for (Map.Entry<String, List<MetricStat>> metricStatsEntry : starTreeMetricStatMap.entrySet()) {
            addEligibleDerivedMetrics(metricStatsEntry.getValue());
            starTreeMetricMap.add(new Metric(metricStatsEntry.getKey(), metricStatsEntry.getValue()));

        }
        return starTreeMetricMap;
    }

    /**
     * Add derived metrics if all associated base metrics are present
     */
    private void addEligibleDerivedMetrics(List<MetricStat> metricStatsList) {
        Set<MetricStat> metricStatsSet = new HashSet<>(metricStatsList);
        for (MetricStat metric : MetricStat.values()) {
            if (metric.isDerivedMetric() && !metricStatsSet.contains(metric)) {
                List<MetricStat> sourceMetrics = metric.getBaseMetrics();
                if (metricStatsSet.containsAll(sourceMetrics)) {
                    metricStatsList.add(metric);
                    metricStatsSet.add(metric);
                }
            }
        }
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
    public List<Metric> getMetrics() {
        return metrics;
    }

    /**
     * Returns the aggregated document count for the star-tree.
     *
     * @return the aggregated document count for the star-tree.
     */
    public int getSegmentAggregatedDocCount() {
        return segmentAggregatedDocCount;
    }

    /**
     * Returns the total number of star tree documents in the segment
     *
     * @return the number of star tree documents in the segment
     */
    public int getStarTreeDocCount() {
        return starTreeDocCount;
    }

    /**
     * Returns the max leaf docs for the star-tree.
     *
     * @return the max leaf docs.
     */
    public int getMaxLeafDocs() {
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

    /**
     * Returns the version with which the star tree is stored in the segments
     * @return star-tree version
     */
    public int getVersion() {
        return version;
    }

    /**
     * Returns the number of nodes in the star tree
     * @return number of nodes in the star tree
     */
    public int getNumberOfNodes() {
        return numberOfNodes;
    }
}

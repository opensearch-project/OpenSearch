/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.index;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.CompositeIndexMetadata;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.ReadDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.StarTreeMetadata;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeFactory;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.StarTreeValuesIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.opensearch.index.codec.composite.composite99.Composite99DocValuesReader.getSortedNumericDocValues;
import static org.opensearch.index.compositeindex.CompositeIndexConstants.SEGMENT_DOCS_COUNT;
import static org.opensearch.index.compositeindex.CompositeIndexConstants.STAR_TREE_DOCS_COUNT;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.fullyQualifiedFieldNameForStarTreeDimensionsDocValues;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues;

/**
 * Concrete class that holds the star tree associated values from the segment
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeValues implements CompositeIndexValues {

    /**
     * Representing the star tree field configuration.
     */
    private final StarTreeField starTreeField;

    /**
     * The root node of the star tree.
     */
    private final StarTreeNode root;

    /**
     * A map containing suppliers for StarTreeValues iterators for dimensions.
     */
    private final Map<String, Supplier<StarTreeValuesIterator>> dimensionValuesIteratorMap;

    /**
     * A map containing suppliers for StarTreeValues iterators for metrics.
     */
    private final Map<String, Supplier<StarTreeValuesIterator>> metricValuesIteratorMap;

    /**
     * A map containing attributes associated with the star tree values.
     */
    private final Map<String, String> attributes;

    /**
     * A metadata for the star-tree
     */
    private final StarTreeMetadata starTreeMetadata;

    /**
     * Constructs a new StarTreeValues object with the provided parameters.
     * Used for testing.
     *
     * @param starTreeField                 The StarTreeField object representing the star tree field configuration.
     * @param root                          The root node of the star tree.
     * @param dimensionValuesIteratorMap A map containing suppliers for StarTreeValues iterators for dimensions.
     * @param metricValuesIteratorMap    A map containing suppliers for StarTreeValues iterators for metrics.
     * @param attributes                    A map containing attributes associated with the star tree values.
     */
    public StarTreeValues(
        StarTreeField starTreeField,
        StarTreeNode root,
        Map<String, Supplier<StarTreeValuesIterator>> dimensionValuesIteratorMap,
        Map<String, Supplier<StarTreeValuesIterator>> metricValuesIteratorMap,
        Map<String, String> attributes,
        StarTreeMetadata compositeIndexMetadata
    ) {
        this.starTreeField = starTreeField;
        this.root = root;
        this.dimensionValuesIteratorMap = dimensionValuesIteratorMap;
        this.metricValuesIteratorMap = metricValuesIteratorMap;
        this.attributes = attributes;
        this.starTreeMetadata = compositeIndexMetadata;
    }

    /**
     * Constructs a new StarTreeValues object by reading the data from the segment
     *
     * @param compositeIndexMetadata     The CompositeIndexMetadata object containing metadata for the composite index.
     * @param compositeIndexDataIn       The IndexInput object for reading the composite index data.
     * @param compositeDocValuesProducer The DocValuesProducer object for producing doc values.
     * @param readState                  The SegmentReadState object representing the state of the segment being read.
     * @throws IOException If an I/O error occurs while reading the data.
     */
    public StarTreeValues(
        CompositeIndexMetadata compositeIndexMetadata,
        IndexInput compositeIndexDataIn,
        DocValuesProducer compositeDocValuesProducer,
        SegmentReadState readState
    ) throws IOException {

        starTreeMetadata = (StarTreeMetadata) compositeIndexMetadata;

        // build skip star node dimensions
        Set<String> skipStarNodeCreationInDims = starTreeMetadata.getSkipStarNodeCreationInDims();

        // build dimensions
        List<Dimension> readDimensions = new ArrayList<>();
        for (String dimension : starTreeMetadata.getDimensionFields()) {
            readDimensions.add(new ReadDimension(dimension));
        }

        // star-tree field
        this.starTreeField = new StarTreeField(
            starTreeMetadata.getCompositeFieldName(),
            readDimensions,
            starTreeMetadata.getMetrics(),
            new StarTreeFieldConfiguration(
                starTreeMetadata.getMaxLeafDocs(),
                skipStarNodeCreationInDims,
                starTreeMetadata.getStarTreeBuildMode()
            )
        );

        this.root = StarTreeFactory.createStarTree(compositeIndexDataIn, starTreeMetadata);

        // get doc id set iterators for metrics and dimensions
        dimensionValuesIteratorMap = new LinkedHashMap<>();
        metricValuesIteratorMap = new LinkedHashMap<>();

        // get doc id set iterators for dimensions
        for (String dimension : starTreeMetadata.getDimensionFields()) {
            dimensionValuesIteratorMap.put(dimension, () -> {
                try {
                    SortedNumericDocValues dimensionSortedNumericDocValues = null;
                    if (readState != null) {
                        FieldInfo dimensionfieldInfo = readState.fieldInfos.fieldInfo(
                            fullyQualifiedFieldNameForStarTreeDimensionsDocValues(starTreeField.getName(), dimension)
                        );
                        if (dimensionfieldInfo != null) {
                            dimensionSortedNumericDocValues = compositeDocValuesProducer.getSortedNumeric(dimensionfieldInfo);
                        }
                    }
                    return new SortedNumericStarTreeValuesIterator(getSortedNumericDocValues(dimensionSortedNumericDocValues));
                } catch (IOException e) {
                    throw new RuntimeException("Error loading dimension StarTreeValuesIterator", e);
                }
            });
        }

        // get doc id set iterators for metrics
        for (Metric metric : starTreeMetadata.getMetrics()) {
            for (MetricStat metricStat : metric.getBaseMetrics()) {
                String metricFullName = fullyQualifiedFieldNameForStarTreeMetricsDocValues(
                    starTreeField.getName(),
                    metric.getField(),
                    metricStat.getTypeName()
                );
                metricValuesIteratorMap.put(metricFullName, () -> {
                    try {
                        SortedNumericDocValues metricSortedNumericDocValues = null;
                        if (readState != null) {
                            FieldInfo metricFieldInfo = readState.fieldInfos.fieldInfo(metricFullName);
                            if (metricFieldInfo != null) {
                                metricSortedNumericDocValues = compositeDocValuesProducer.getSortedNumeric(metricFieldInfo);
                            }
                        }
                        return new SortedNumericStarTreeValuesIterator(getSortedNumericDocValues(metricSortedNumericDocValues));
                    } catch (IOException e) {
                        throw new RuntimeException("Error loading metric DocIdSetIterator", e);
                    }
                });
            }
        }

        // create star-tree attributes map

        // Create an unmodifiable view of the map
        attributes = Map.of(
            SEGMENT_DOCS_COUNT,
            String.valueOf(starTreeMetadata.getSegmentAggregatedDocCount()),
            STAR_TREE_DOCS_COUNT,
            String.valueOf(starTreeMetadata.getStarTreeDocCount())
        );

    }

    @Override
    public CompositeIndexValues getValues() {
        return this;
    }

    /**
     * Returns an object representing the star tree field configuration.
     *
     * @return The StarTreeField object representing the star tree field configuration.
     */
    public StarTreeField getStarTreeField() {
        return starTreeField;
    }

    /**
     * Returns the root node of the star tree.
     *
     * @return The root node of the star tree.
     */
    public StarTreeNode getRoot() {
        return root;
    }

    /**
     * Returns the map containing attributes associated with the star tree values.
     *
     * @return The map containing attributes associated with the star tree values.
     */
    public Map<String, String> getAttributes() {
        return attributes;
    }

    /**
     * Returns the StarTreeValues iterator for the specified dimension.
     *
     * @param dimension The name of the dimension.
     * @return The StarTreeValuesIterator for the specified dimension.
     */
    public StarTreeValuesIterator getDimensionValuesIterator(String dimension) {

        if (dimensionValuesIteratorMap.containsKey(dimension)) {
            return dimensionValuesIteratorMap.get(dimension).get();
        }

        throw new IllegalArgumentException("dimension [" + dimension + "] does not exist in the segment.");
    }

    /**
     * Returns the StarTreeValues iterator for the specified fully qualified metric name.
     *
     * @param fullyQualifiedMetricName The fully qualified name of the metric.
     * @return The StarTreeValuesIterator for the specified fully qualified metric name.
     */
    public StarTreeValuesIterator getMetricValuesIterator(String fullyQualifiedMetricName) {

        if (metricValuesIteratorMap.containsKey(fullyQualifiedMetricName)) {
            return metricValuesIteratorMap.get(fullyQualifiedMetricName).get();
        }

        throw new IllegalArgumentException("metric [" + fullyQualifiedMetricName + "] does not exist in the segment.");
    }

    public int getStarTreeDocumentCount() {
        return starTreeMetadata.getStarTreeDocCount();
    }
}

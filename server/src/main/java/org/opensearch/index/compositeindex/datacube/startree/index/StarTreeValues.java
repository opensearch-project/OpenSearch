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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.CompositeIndexMetadata;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.ReadDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.MetricEntry;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.StarTreeMetadata;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTree;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private final StarTreeField starTreeField;
    private final StarTreeNode root;
    private final Map<String, DocIdSetIterator> dimensionDocValuesIteratorMap;
    private final Map<String, DocIdSetIterator> metricDocValuesIteratorMap;
    private final Map<String, String> attributes;

    public StarTreeValues(
        StarTreeField starTreeField,
        StarTreeNode root,
        Map<String, DocIdSetIterator> dimensionDocValuesIteratorMap,
        Map<String, DocIdSetIterator> metricDocValuesIteratorMap,
        Map<String, String> attributes
    ) {
        this.starTreeField = starTreeField;
        this.root = root;
        this.dimensionDocValuesIteratorMap = dimensionDocValuesIteratorMap;
        this.metricDocValuesIteratorMap = metricDocValuesIteratorMap;
        this.attributes = attributes;
    }

    public StarTreeValues(
        CompositeIndexMetadata compositeIndexMetadata,
        IndexInput compositeIndexIn,
        DocValuesProducer compositeDocValuesProducer,
        SegmentReadState readState
    ) throws IOException {

        StarTreeMetadata starTreeMetadata = (StarTreeMetadata) compositeIndexMetadata;

        // build skip star node dimensions
        Set<String> skipStarNodeCreationInDims = starTreeMetadata.getSkipStarNodeCreationInDims();

        // build dimensions
        List<Dimension> readDimensions = new ArrayList<>();
        for (String dimension : starTreeMetadata.getDimensionFields()) {
            readDimensions.add(new ReadDimension(dimension));
        }

        // build metrics
        Map<String, Metric> starTreeMetricMap = new LinkedHashMap<>();
        for (MetricEntry metricEntry : starTreeMetadata.getMetricEntries()) {
            String metricName = metricEntry.getMetricFieldName();
            Metric metric = starTreeMetricMap.computeIfAbsent(metricName, field -> new Metric(field, new ArrayList<>()));
            metric.getMetrics().add(metricEntry.getMetricStat());
        }
        List<Metric> starTreeMetrics = new ArrayList<>(starTreeMetricMap.values());

        // star-tree field
        this.starTreeField = new StarTreeField(
            starTreeMetadata.getCompositeFieldName(),
            readDimensions,
            starTreeMetrics,
            new StarTreeFieldConfiguration(
                starTreeMetadata.getMaxLeafDocs(),
                skipStarNodeCreationInDims,
                starTreeMetadata.getStarTreeBuildMode()
            )
        );

        StarTree starTree = new StarTree(compositeIndexIn, starTreeMetadata);
        this.root = starTree.getRoot();

        // get doc id set iterators for metrics and dimensions
        dimensionDocValuesIteratorMap = new LinkedHashMap<>();
        metricDocValuesIteratorMap = new LinkedHashMap<>();

        // get doc id set iterators for dimensions
        for (String dimension : starTreeMetadata.getDimensionFields()) {
            SortedNumericDocValues dimensionSortedNumericDocValues = null;
            if (readState != null) {
                FieldInfo dimensionfieldInfo = readState.fieldInfos.fieldInfo(
                    fullyQualifiedFieldNameForStarTreeDimensionsDocValues(starTreeField.getName(), dimension)
                );
                if (dimensionfieldInfo != null) {
                    dimensionSortedNumericDocValues = compositeDocValuesProducer.getSortedNumeric(dimensionfieldInfo);
                }
            }

            dimensionDocValuesIteratorMap.put(dimension, getSortedNumericDocValues(dimensionSortedNumericDocValues));
        }

        // get doc id set iterators for metrics
        for (MetricEntry metricEntry : starTreeMetadata.getMetricEntries()) {
            String metricFullName = fullyQualifiedFieldNameForStarTreeMetricsDocValues(
                starTreeField.getName(),
                metricEntry.getMetricFieldName(),
                metricEntry.getMetricStat().getTypeName()
            );

            SortedNumericDocValues metricSortedNumericDocValues = null;
            if (readState != null) {
                FieldInfo metricFieldInfo = readState.fieldInfos.fieldInfo(metricFullName);
                if (metricFieldInfo != null) {
                    metricSortedNumericDocValues = compositeDocValuesProducer.getSortedNumeric(metricFieldInfo);
                }
            }
            metricDocValuesIteratorMap.put(metricFullName, getSortedNumericDocValues(metricSortedNumericDocValues));

        }

        // create star-tree attributes map
        attributes = new HashMap<>();
        attributes.put(SEGMENT_DOCS_COUNT, String.valueOf(starTreeMetadata.getSegmentAggregatedDocCount()));
        attributes.put(STAR_TREE_DOCS_COUNT, String.valueOf(starTreeMetadata.getStarTreeDocCount()));

    }

    @Override
    public CompositeIndexValues getValues() {
        return this;
    }

    public StarTreeField getStarTreeField() {
        return starTreeField;
    }

    public StarTreeNode getRoot() {
        return root;
    }

    public Map<String, DocIdSetIterator> getDimensionDocValuesIteratorMap() {
        return dimensionDocValuesIteratorMap;
    }

    public Map<String, DocIdSetIterator> getMetricDocValuesIteratorMap() {
        return metricDocValuesIteratorMap;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }
}
